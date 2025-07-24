import asyncio
import logging
import os
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Literal

import hishel
import httpx
from aiolimiter import AsyncLimiter
from async_lru import alru_cache
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from fca_mcp.models import (
    ContributionsResponse,
    DebateParent,
    ElasticDocument,
    FCAAuthorisedFirm,
    FCAConsultationPaper,
    FCAEnforcementNotice,
    FCAHandbookSection,
    FCAIndividual,
    FCAPolicyStatement,
    FCAProduct,
    ParliamentaryQuestion,
    ParliamentaryQuestionsResponse,
)
from fca_mcp.settings import FCAmcpSettings, settings

logger = logging.getLogger(__name__)

HANSARD_BASE_URL = "https://hansard-api.parliament.uk"
PQS_BASE_URL = "https://questions-statements-api.parliament.uk/api"


_http_client_rate_limiter = AsyncLimiter(max_rate=1, time_period=2.0)  # Very conservative for FCA API


async def cached_limited_get(*args, **kwargs) -> httpx.Response:
    """
    A wrapper around httpx.get that caches the result and limits the rate of requests.
    """
    # Use /tmp for cache in Lambda environment
    if os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
        # In Lambda, use tempfile to get the temp directory securely
        cache_dir = str(Path(tempfile.gettempdir()) / ".cache" / "hishel")
    else:
        cache_dir = ".cache/hishel"

    async with (
        hishel.AsyncCacheClient(
            timeout=30,
            headers={"User-Agent": "parliament-mcp"},
            storage=hishel.AsyncFileStorage(base_path=cache_dir, ttl=timedelta(days=1).total_seconds()),
            transport=httpx.AsyncHTTPTransport(retries=3),
        ) as client,
        _http_client_rate_limiter,
    ):
        return await client.get(*args, **kwargs)


@alru_cache(maxsize=128, typed=True)
async def load_section_trees(date: str, house: Literal["Commons", "Lords"]) -> dict[int, dict]:
    """
    Loads the debate hierarchy (i.e. section trees) for a given date and house.

    Note: This sits outside the hansard loader because we don't want to cache 'self'

    Args:
        date: The date to load the debate hierarchy for.
        house: The house to load the debate hierarchy for.

    Returns:
        A dictionary of debate parents. Maps both the section id and the external id to the section data.
    """
    url = f"{HANSARD_BASE_URL}/overview/sectionsforday.json"
    response = await cached_limited_get(url, params={"house": house, "date": date})
    response.raise_for_status()
    sections = response.json()

    section_tree_items = []
    for section in sections:
        url = f"{HANSARD_BASE_URL}/overview/sectiontrees.json"
        response = await cached_limited_get(url, params={"section": section, "date": date, "house": house})
        response.raise_for_status()
        section_tree = response.json()
        for item in section_tree:
            section_tree_items.extend(item.get("SectionTreeItems", []))

    # Create a mapping of ID to item for easy lookup
    # Map both the section id and the external id to the section data
    section_tree_map = {}
    for item in section_tree_items:
        section_tree_map[item["Id"]] = item
        section_tree_map[item["ExternalId"]] = item
    return section_tree_map


class ElasticDataLoader:
    """Base class for loading data into Elasticsearch with progress tracking."""

    def __init__(self, elastic_client: AsyncElasticsearch, index_name: str):
        self.elastic_client = elastic_client
        self.index_name = index_name
        self.progress: Progress | None = None

    async def get_total_results(self, url: str, params: dict, count_key: str = "TotalResultCount") -> int:
        """Get total results count from API endpoint"""
        count_params = {**params, "take": 1, "skip": 0}
        response = await cached_limited_get(url, params=count_params)
        response.raise_for_status()
        data = response.json()
        if count_key not in data:
            msg = f"Count key {count_key} not found in response: {data}"
            raise ValueError(msg)
        return data[count_key]

    @contextmanager
    def progress_context(self) -> Generator[Progress, None, None]:
        """Context manager for rich progress bar display."""
        if self.progress is not None:
            yield self.progress

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TextColumn("Elapsed: "),
            TimeElapsedColumn(),
            TextColumn("Remaining: "),
            TimeRemainingColumn(),
            expand=True,
        ) as progress:
            self.progress = progress
            yield progress
            self.progress.refresh()
        self.progress = None

    async def store_in_elastic(self, data: list[ElasticDocument]) -> None:
        """Bulk store documents in Elasticsearch with retries."""
        try:
            actions = [
                {
                    "_op_type": "index",
                    "_index": self.index_name,
                    "_id": item.document_uri,
                    "_source": item.model_dump(mode="json"),
                }
                for item in data
            ]

            await async_bulk(
                self.elastic_client,
                actions=actions,
                max_retries=3,
            )
        except BulkIndexError as e:
            raise e from e


class ElasticHansardLoader(ElasticDataLoader):
    """Loader for Hansard parliamentary debate contributions."""

    def __init__(
        self,
        page_size: int = 100,
        *args,
        **kwargs,
    ):
        self.page_size = page_size
        super().__init__(*args, **kwargs)

    async def load_all_contributions(
        self,
        from_date: str = "2020-01-01",
        to_date: str = "2020-02-10",
    ) -> None:
        """Load all contribution types concurrently."""
        contribution_types = ["Spoken", "Written", "Corrections", "Petitions"]
        with self.progress_context():
            async with asyncio.TaskGroup() as tg:
                for contrib_type in contribution_types:
                    tg.create_task(self.load_contributions_by_type(contrib_type, from_date, to_date))

    async def load_contributions_by_type(
        self,
        contribution_type: Literal["Spoken", "Written", "Corrections", "Petitions"] = "Spoken",
        from_date: str = "2025-01-01",
        to_date: str = "2025-01-10",
    ) -> None:
        """Load specific contribution type with pagination."""
        base_params = {
            "orderBy": "SittingDateAsc",
            "startDate": from_date,
            "endDate": to_date,
        }

        url = f"{HANSARD_BASE_URL}/search/contributions/{contribution_type}.json"
        total_results = await self.get_total_results(url, base_params | {"take": 1, "skip": 0})
        task = self.progress.add_task(
            f"Loading '{contribution_type}' contributions",
            total=total_results,
            completed=0,
        )
        if total_results == 0:
            self.progress.update(task, completed=total_results)
            return

        semaphore = asyncio.Semaphore(5)

        async def process_page(query_params: dict):
            """Fetch and process a single page"""
            try:
                async with semaphore:
                    response = await cached_limited_get(url, params=query_params)
                    response.raise_for_status()
                    page_data = response.json()

                    contributions = ContributionsResponse.model_validate(page_data)
                    valid_contributions = [c for c in contributions.Results if len(c.ContributionTextFull) > 0]

                    for contribution in valid_contributions:
                        contribution.debate_parents = await self.get_debate_parents(
                            contribution.SittingDate.strftime("%Y-%m-%d"),
                            contribution.House,
                            contribution.DebateSectionExtId,
                        )

                    await self.store_in_elastic(valid_contributions)
                    self.progress.update(task, advance=len(contributions.Results))
            except Exception:
                logger.exception("Failed to process page - %s", query_params)

        # TaskGroup with one task per page
        async with asyncio.TaskGroup() as tg:
            for skip in range(0, total_results, self.page_size):
                tg.create_task(process_page(base_params | {"take": self.page_size, "skip": skip}))

    async def get_debate_parents(
        self, date: str, house: Literal["Commons", "Lords"], debate_ext_id: str
    ) -> list[DebateParent]:
        """Retrieve parent debate hierarchy for a contribution."""

        try:
            section_tree_for_date = await load_section_trees(date, house)

            # use the external id rather than the section id because external ids are more stable
            next_id = debate_ext_id
            debate_parents = []
            while next_id is not None:
                parent = DebateParent.model_validate(section_tree_for_date[next_id])
                debate_parents.append(parent)
                next_id = parent.ParentId
        except Exception:
            logger.exception(
                "Failed to get debate parents for debate id - %s",
                debate_ext_id,
            )
            return []
        else:
            return debate_parents


class ElasticParliamentaryQuestionLoader(ElasticDataLoader):
    """
    Handles the loading and processing of parliamentary questions from the API.

    This class manages HTTP requests with rate limiting and caching, and handles
    the fetching of both summarised and full question content when needed.
    """

    def __init__(
        self,
        page_size: int = 50,
        *args,
        **kwargs,
    ):
        self.page_size = page_size
        super().__init__(*args, **kwargs)

    async def enrich_question(self, question: ParliamentaryQuestion) -> ParliamentaryQuestion:
        """
        Fetch the full version of a question when the summary is truncated.

        Args:
            question_id: The unique identifier of the question

        Returns:
            ParliamentaryQuestion: The full question data or original if fetch fails

        Raises:
            ValueError: If no original question is stored
        """
        try:
            response = await cached_limited_get(
                f"{PQS_BASE_URL}/writtenquestions/questions/{question.id}",
                params={"expandMember": "true"},
            )
            response.raise_for_status()

            data = response.json()
            new_question = ParliamentaryQuestion(**data["value"])
            question.questionText = new_question.questionText
            question.answerText = new_question.answerText
        except Exception:
            logger.exception("Failed to fetch full question - %s", question.id)
        return question

    async def load_questions_for_date_range(
        self,
        from_date: str = "2020-01-01",
        to_date: str = "2020-02-10",
    ) -> None:
        """
        Load questions within the specified date range, checking both tabled and answered dates.
        """
        url = f"{PQS_BASE_URL}/writtenquestions/questions"

        semaphore = asyncio.Semaphore(5)
        seen_ids = set()

        async def process_page(query_params: dict, task_id: int):
            """Fetch and process a single page"""
            try:
                async with semaphore:
                    response = await cached_limited_get(url, params=query_params)
                    response.raise_for_status()
                    page_data = response.json()

                    response = ParliamentaryQuestionsResponse.model_validate(page_data)
                    valid_questions = []

                    for question in response.questions:
                        if question.id in seen_ids:
                            continue

                        seen_ids.add(question.id)

                        if question.is_truncated:
                            enriched_question = await self.enrich_question(question)
                            valid_questions.append(enriched_question)
                        else:
                            valid_questions.append(question)

                    await self.store_in_elastic(valid_questions)
                    self.progress.update(task_id, advance=len(response.questions))
            except Exception:
                logger.exception("Failed to process page - %s", query_params)

        with self.progress_context():
            tabled_task_id = self.progress.add_task("Loading 'tabled' questions", total=0, completed=0, start=True)
            answered_task_id = self.progress.add_task("Loading 'answered' questions", total=0, completed=0, start=False)

            async with asyncio.TaskGroup() as tg:
                questions_tabled_params = {
                    "tabledWhenFrom": from_date,
                    "tabledWhenTo": to_date,
                    "expandMember": "true",
                    "take": self.page_size,
                }
                total_results = await self.get_total_results(url, questions_tabled_params, count_key="totalResults")
                self.progress.start_task(tabled_task_id)
                self.progress.update(tabled_task_id, total=total_results)
                for skip in range(0, total_results, self.page_size):
                    tg.create_task(process_page(questions_tabled_params | {"skip": skip}, tabled_task_id))

            async with asyncio.TaskGroup() as tg:
                questions_answered_params = {
                    "answeredWhenFrom": from_date,
                    "answeredWhenTo": to_date,
                    "expandMember": "true",
                    "take": self.page_size,
                }
                total_results = await self.get_total_results(url, questions_answered_params, count_key="totalResults")
                self.progress.start_task(answered_task_id)
                self.progress.update(answered_task_id, total=total_results)
                for skip in range(0, total_results, self.page_size):
                    tg.create_task(process_page(questions_answered_params | {"skip": skip}, answered_task_id))


class ElasticFCAHandbookLoader(ElasticDataLoader):
    """Loader for FCA Handbook sections and rules."""
    
    def __init__(self, elastic_client: AsyncElasticsearch, index_name: str):
        super().__init__(elastic_client, index_name)
        self.base_url = settings.FCA_HANDBOOK_API_BASE_URL
        
    async def load_handbook_sections(self):
        """Load all FCA Handbook sections."""
        logger.info("Loading FCA Handbook sections...")
        
        # For now, implement a basic loader that demonstrates the structure
        # In a real implementation, this would fetch from FCA APIs or scrape the handbook
        sample_sections = [
            {
                "section_id": "PRIN_1",
                "chapter": "PRIN",
                "section_number": "1",
                "title": "The Principles for Businesses",
                "content": "A firm must conduct its business with integrity.",
                "content_type": "rule",
                "is_current": True,
                "source_url": "https://www.handbook.fca.org.uk/handbook/PRIN/1/"
            },
            {
                "section_id": "COBS_2_1",
                "chapter": "COBS", 
                "section_number": "2.1",
                "title": "Acting honestly, fairly and professionally",
                "content": "A firm must act honestly, fairly and professionally in accordance with the best interests of its client.",
                "content_type": "rule",
                "is_current": True,
                "source_url": "https://www.handbook.fca.org.uk/handbook/COBS/2/1/"
            }
        ]
        
        sections = []
        for section_data in sample_sections:
            section = FCAHandbookSection(**section_data)
            sections.append(section)
        
        await self.store_in_elastic(sections)


class ElasticFCAPolicyStatementLoader(ElasticDataLoader):
    """Loader for FCA Policy Statements."""
    
    def __init__(self, elastic_client: AsyncElasticsearch, index_name: str):
        super().__init__(elastic_client, index_name)
        
    async def load_policy_statements(self, from_date: str | None = None, to_date: str | None = None):
        """Load FCA Policy Statements within date range."""
        logger.info("Loading FCA Policy Statements...")
        
        # Sample policy statements - in practice would fetch from FCA website/API
        sample_policies = [
            {
                "ps_number": "PS24/1",
                "title": "Consumer Duty: Implementation and feedback",
                "publication_date": "2024-01-15T00:00:00Z",
                "summary": "Our final policy on Consumer Duty implementation requirements",
                "content": "This policy statement sets out our final rules on Consumer Duty...",
                "policy_area": "Consumer Protection"
            },
            {
                "ps_number": "PS24/2", 
                "title": "Operational resilience: Critical third parties",
                "publication_date": "2024-02-20T00:00:00Z",
                "summary": "Policy on operational resilience requirements for critical third parties",
                "content": "We are introducing new requirements for critical third party service providers...",
                "policy_area": "Operational Resilience"
            }
        ]
        
        policies = []
        for policy_data in sample_policies:
            policy = FCAPolicyStatement(**policy_data)
            policies.append(policy)
            
        await self.store_in_elastic(policies)


class ElasticFCAConsultationPaperLoader(ElasticDataLoader):
    """Loader for FCA Consultation Papers."""
    
    def __init__(self, elastic_client: AsyncElasticsearch, index_name: str):
        super().__init__(elastic_client, index_name)
        
    async def load_consultation_papers(self, from_date: str | None = None, to_date: str | None = None):
        """Load FCA Consultation Papers within date range."""
        logger.info("Loading FCA Consultation Papers...")
        
        # Sample consultation papers
        sample_cps = [
            {
                "cp_number": "CP24/1",
                "title": "Reviewing the Senior Managers and Certification Regime",
                "publication_date": "2024-03-01T00:00:00Z",
                "consultation_closes": "2024-05-01T00:00:00Z",
                "summary": "We are reviewing the Senior Managers and Certification Regime",
                "content": "This consultation paper seeks feedback on proposed changes to the SM&CR...",
                "policy_area": "Senior Management"
            }
        ]
        
        cps = []
        for cp_data in sample_cps:
            cp = FCAConsultationPaper(**cp_data)
            cps.append(cp)
            
        await self.store_in_elastic(cps)


class ElasticFCAAuthorisedFirmsLoader(ElasticDataLoader):
    """Loader for FCA Authorised Firms register with comprehensive API integration."""
    
    def __init__(self, elastic_client: AsyncElasticsearch, index_name: str):
        super().__init__(elastic_client, index_name)
        
    async def _api_call(self, endpoint: str, headers: dict) -> dict | None:
        """Make FCA API call with proper status code handling."""
        try:
            response = await cached_limited_get(endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            status = data.get("Status", "")
            message = data.get("Message", "")
            
            # Success codes that contain data
            if status.startswith("FSR-API-") and any(keyword in message for keyword in [
                "Ok", "Found", "successful", "Success"
            ]):
                return data
            
            # Valid "not found" responses - these are OK, just no data available
            elif status.startswith("FSR-API-") and any(keyword in message for keyword in [
                "not found", "Not Found", "No search result found"
            ]):
                return {"Status": status, "Message": message, "Data": None}
            
            # Any other FSR-API response is valid (just log and return)
            elif status.startswith("FSR-API-"):
                logger.debug(f"API response for {endpoint}: {status} - {message}")
                return data
            
            else:
                logger.warning(f"Unexpected API response for {endpoint}: {status} - {message}")
                return None
                
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error {e.response.status_code} for {endpoint}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed API call to {endpoint}: {e}")
            return None
    
    async def _get_firm_names(self, frn: str, headers: dict) -> list[str]:
        """Get all trading names and brand names for a firm."""
        endpoint = f"{settings.FCA_API_BASE_URL}/Firm/{frn}/Names"
        data = await self._api_call(endpoint, headers)
        
        names = []
        if data and data.get("Data"):
            for name_group in data["Data"]:
                # Current names
                if "Current Names" in name_group:
                    for name_info in name_group["Current Names"]:
                        if name_info.get("Name"):
                            names.append(name_info["Name"])
                # Previous names (for historical context)
                if "Previous Names" in name_group:
                    for name_info in name_group["Previous Names"]:
                        if name_info.get("Name"):
                            names.append(f"{name_info['Name']} (Historical)")
        
        return names
    
    async def _get_firm_address(self, frn: str, headers: dict) -> dict:
        """Get comprehensive address information for a firm."""
        endpoint = f"{settings.FCA_API_BASE_URL}/Firm/{frn}/Address"
        data = await self._api_call(endpoint, headers)
        
        address_info = {
            "address_line_1": "",
            "address_line_2": "",
            "address_line_3": "",
            "address_line_4": "",
            "city": "",
            "county": "",
            "postcode": "",
            "country": "",
            "telephone": "",
            "website": ""
        }
        
        if data and data.get("Data"):
            # Prefer Principal Place of Business, fallback to first address
            ppob_address = None
            first_address = None
            
            for addr in data["Data"]:
                if addr.get("Address Type") == "Principal Place of Business":
                    ppob_address = addr
                    break
                elif first_address is None:
                    first_address = addr
            
            selected_addr = ppob_address or first_address
            if selected_addr:
                address_info.update({
                    "address_line_1": selected_addr.get("Address Line 1", ""),
                    "address_line_2": selected_addr.get("Address Line 2", ""),
                    "address_line_3": selected_addr.get("Address Line 3", ""),
                    "address_line_4": selected_addr.get("Address Line 4", ""),
                    "city": selected_addr.get("Town", ""),
                    "county": selected_addr.get("County", ""),
                    "postcode": selected_addr.get("Postcode", ""),
                    "country": selected_addr.get("Country", ""),
                    "telephone": selected_addr.get("Phone Number", ""),
                    "website": selected_addr.get("Website Address", "")
                })
        
        return address_info
    
    async def _get_firm_permissions(self, frn: str, headers: dict) -> list[str]:
        """Get detailed permissions and activities for a firm."""
        endpoint = f"{settings.FCA_API_BASE_URL}/Firm/{frn}/Permissions"
        data = await self._api_call(endpoint, headers)
        
        permissions = []
        if data and data.get("Data"):
            # The permissions are structured as nested objects
            for activity, details in data["Data"].items():
                permissions.append(activity)
                
                # Extract any limitations
                if isinstance(details, list):
                    for detail_group in details:
                        if isinstance(detail_group, dict):
                            for key, values in detail_group.items():
                                if "Limitation" in key and isinstance(values, list):
                                    for limitation in values:
                                        if limitation not in ["Valid limitation not present", "Limitation Not Found"]:
                                            permissions.append(f"LIMITATION: {limitation}")
        
        return permissions
    
    async def _get_firm_individuals(self, frn: str, headers: dict) -> list[dict]:
        """Get key individuals associated with a firm."""
        endpoint = f"{settings.FCA_API_BASE_URL}/Firm/{frn}/Individuals"
        data = await self._api_call(endpoint, headers)
        
        individuals = []
        if data and data.get("Data"):
            for person in data["Data"]:
                individuals.append({
                    "name": person.get("Name", ""),
                    "irn": person.get("IRN", ""),
                    "status": person.get("Status", ""),
                    "api_url": person.get("URL", "")
                })
        
        return individuals
    
    async def _get_firm_requirements(self, frn: str, headers: dict) -> list[str]:
        """Get regulatory requirements and restrictions for a firm."""
        endpoint = f"{settings.FCA_API_BASE_URL}/Firm/{frn}/Requirements"
        data = await self._api_call(endpoint, headers)
        
        requirements = []
        if data and data.get("Data"):
            for req in data["Data"]:
                # Extract the requirement text (which might be in various keys)
                for key, value in req.items():
                    if key not in ["Effective Date", "Requirement Reference", "Financial Promotions Requirement", 
                                  "Financial Promotions Investment Types"] and isinstance(value, str) and value:
                        requirements.append(f"{key}: {value}")
        
        return requirements
    
    async def _get_firm_disciplinary_history(self, frn: str, headers: dict) -> list[dict]:
        """Get disciplinary history for a firm."""
        endpoint = f"{settings.FCA_API_BASE_URL}/Firm/{frn}/DisciplinaryHistory"
        data = await self._api_call(endpoint, headers)
        
        disciplinary_actions = []
        if data and data.get("Data"):
            for action in data["Data"]:
                disciplinary_actions.append({
                    "action_type": action.get("TypeofAction", ""),
                    "enforcement_type": action.get("EnforcementType", ""),
                    "description": action.get("TypeofDescription", ""),
                    "effective_date": action.get("ActionEffectiveFrom", "")
                })
        
        return disciplinary_actions
    
    async def _get_enhanced_firm_data(self, frn: str, base_firm_data: dict, headers: dict) -> FCAAuthorisedFirm:
        """Get comprehensive firm data using multiple API endpoints."""
        
        # Run all API calls concurrently for efficiency
        async with asyncio.TaskGroup() as tg:
            names_task = tg.create_task(self._get_firm_names(frn, headers))
            address_task = tg.create_task(self._get_firm_address(frn, headers))
            permissions_task = tg.create_task(self._get_firm_permissions(frn, headers))
            individuals_task = tg.create_task(self._get_firm_individuals(frn, headers))
            requirements_task = tg.create_task(self._get_firm_requirements(frn, headers))
            disciplinary_task = tg.create_task(self._get_firm_disciplinary_history(frn, headers))
        
        # Collect results
        trading_names = names_task.result()
        address_info = address_task.result()
        permissions = permissions_task.result()
        individuals = individuals_task.result()
        requirements = requirements_task.result()
        disciplinary_history = disciplinary_task.result()
        
        # Create comprehensive firm record
        firm = FCAAuthorisedFirm(
            firm_reference_number=frn,
            firm_name=base_firm_data.get("Organisation Name", ""),
            trading_names=trading_names,
            firm_status=base_firm_data.get("Status", ""),
            permissions=permissions,
            address_line_1=address_info["address_line_1"],
            city=address_info["city"],
            postcode=address_info["postcode"],
            country=address_info["country"],
            telephone=address_info["telephone"],
            business_type=base_firm_data.get("Business Type", ""),
            companies_house_number=base_firm_data.get("Companies House Number", ""),
            client_money_permission=base_firm_data.get("Client Money Permission", ""),
            psd_status=base_firm_data.get("PSD / EMD Status", ""),
            # Additional comprehensive data
            website=address_info["website"],
            key_individuals=[ind["name"] for ind in individuals],
            regulatory_requirements=requirements,
            disciplinary_history=[f"{action['action_type']}: {action['description']}" for action in disciplinary_history],
            sub_status=base_firm_data.get("Sub-Status", ""),
            mlrs_status=base_firm_data.get("MLRs Status", ""),
            exceptional_info=[info.get("Exceptional Info Body", "") for info in base_firm_data.get("Exceptional Info Details", [])],
        )
        
        return firm
        
    async def load_authorised_firms(self, limit: int = 500):
        """Load comprehensive FCA Authorised Firms data using all available API endpoints."""
        logger.info("Loading comprehensive FCA Authorised Firms data...")
        
        headers = {
            "x-auth-email": settings.FCA_API_EMAIL,
            "x-auth-key": settings.FCA_API_KEY,
            "Content-Type": "application/json"
        }
        
        # Use specific working search approach
        # From testing: broad terms often return no results, so we'll use targeted approach
        search_terms = [
            "ltd", "limited", "plc", "llp", "limited liability"  # Company suffixes that exist
        ]
        
        # Also try direct FRN processing if we have known working ones
        known_working_frns = ["615820"]  # From our testing
        
        all_firms = {}  # Use dict to avoid duplicates by FRN
        
        with self.progress_context() as progress:
            search_task = progress.add_task("Searching for firms", total=len(search_terms))
            
            for term in search_terms:
                try:
                    # Search for firms
                    search_url = f"{settings.FCA_API_BASE_URL}/Search"
                    search_params = {"q": term, "type": "firm"}
                    
                    response = await cached_limited_get(search_url, params=search_params, headers=headers)
                    response.raise_for_status()
                    search_data = response.json()
                    
                    # Handle both successful searches and valid "no results" responses
                    if search_data.get("Status") == "FSR-API-04-01-00" and search_data.get("Data"):
                        for firm_summary in search_data["Data"][:20]:  # Limit per search term
                            frn = firm_summary.get("Reference Number")
                            if frn and frn not in all_firms:
                                all_firms[frn] = firm_summary
                    elif search_data.get("Status") == "FSR-API-04-01-11":
                        logger.debug(f"No search results for term '{term}' - this is normal")
                    else:
                        logger.warning(f"Unexpected search response for '{term}': {search_data.get('Status')} - {search_data.get('Message')}")
                    
                    progress.update(search_task, advance=1)
                    
                except Exception as e:
                    logger.error(f"Failed to search for '{term}' firms: {e}")
                    progress.update(search_task, advance=1)
                    continue
            
            # Add known working FRNs directly (bypassing search limitations)
            for frn in known_working_frns:
                if frn not in all_firms:
                    all_firms[frn] = {"Reference Number": frn, "Name": f"Firm {frn}", "Type of business or Individual": "Firm"}
                    logger.info(f"Added known working FRN: {frn}")
            
            # Now get comprehensive data for each firm
            firm_frns = list(all_firms.keys())[:limit]
            detail_task = progress.add_task("Loading detailed firm data", total=len(firm_frns))
            
            # Process firms in smaller batches to respect rate limits
            batch_size = 3
            final_firms = []
            
            for i in range(0, len(firm_frns), batch_size):
                batch_frns = firm_frns[i:i + batch_size]
                
                async with asyncio.TaskGroup() as tg:
                    batch_tasks = []
                    for frn in batch_frns:
                        batch_tasks.append(tg.create_task(self._process_single_firm(frn, headers)))
                
                # Collect results from batch
                for task in batch_tasks:
                    firm = task.result()
                    if firm:
                        final_firms.append(firm)
                    progress.update(detail_task, advance=1)
        
        logger.info(f"Loaded {len(final_firms)} comprehensive FCA firms with full API data")
        await self.store_in_elastic(final_firms)
    
    async def _process_single_firm(self, frn: str, headers: dict) -> FCAAuthorisedFirm | None:
        """Process a single firm to get comprehensive data."""
        try:
            # Get basic firm details first
            detail_url = f"{settings.FCA_API_BASE_URL}/Firm/{frn}"
            detail_data = await self._api_call(detail_url, headers)
            
            if detail_data and detail_data.get("Data"):
                base_firm_data = detail_data["Data"][0]
                return await self._get_enhanced_firm_data(frn, base_firm_data, headers)
            
        except Exception as e:
            logger.warning(f"Failed to process firm {frn}: {e}")
        
        return None


class ElasticFCAIndividualsLoader(ElasticDataLoader):
    """Loader for FCA Individual Register data."""
    
    def __init__(self, elastic_client: AsyncElasticsearch, index_name: str):
        super().__init__(elastic_client, index_name)
    
    async def _api_call(self, endpoint: str, headers: dict) -> dict | None:
        """Make FCA API call with proper status code handling."""
        try:
            response = await cached_limited_get(endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            status = data.get("Status", "")
            message = data.get("Message", "")
            
            # Success codes that contain data
            if status.startswith("FSR-API-") and any(keyword in message for keyword in [
                "Ok", "Found", "successful", "Success"
            ]):
                return data
            
            # Valid "not found" responses - these are OK, just no data available
            elif status.startswith("FSR-API-") and any(keyword in message for keyword in [
                "not found", "Not Found", "No search result found"
            ]):
                return {"Status": status, "Message": message, "Data": None}
            
            # Any other FSR-API response is valid (just log and return)
            elif status.startswith("FSR-API-"):
                logger.debug(f"API response for {endpoint}: {status} - {message}")
                return data
            
            else:
                logger.warning(f"Unexpected API response for {endpoint}: {status} - {message}")
                return None
                
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error {e.response.status_code} for {endpoint}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed API call to {endpoint}: {e}")
            return None
    
    async def load_individuals_from_firms(self, firm_frns: list[str]):
        """Load individual data discovered from firm records."""
        logger.info("Loading FCA Individual data from firm associations...")
        
        headers = {
            "x-auth-email": settings.FCA_API_EMAIL,
            "x-auth-key": settings.FCA_API_KEY,
            "Content-Type": "application/json"
        }
        
        all_individuals = {}
        
        with self.progress_context() as progress:
            firm_task = progress.add_task("Processing firms for individuals", total=len(firm_frns))
            
            for frn in firm_frns:
                try:
                    # Get individuals associated with this firm
                    individuals_url = f"{settings.FCA_API_BASE_URL}/Firm/{frn}/Individuals"
                    individuals_data = await self._api_call(individuals_url, headers)
                    
                    if individuals_data and individuals_data.get("Data"):
                        for person in individuals_data["Data"]:
                            irn = person.get("IRN")
                            if irn and irn not in all_individuals:
                                # Get detailed individual information
                                individual_detail = await self._get_individual_details(irn, headers)
                                if individual_detail:
                                    all_individuals[irn] = individual_detail
                    
                    progress.update(firm_task, advance=1)
                    
                except Exception as e:
                    logger.warning(f"Failed to get individuals for firm {frn}: {e}")
                    progress.update(firm_task, advance=1)
                    continue
        
        final_individuals = list(all_individuals.values())
        logger.info(f"Loaded {len(final_individuals)} individual records")
        
        # Store individuals (you'd need to create an FCAIndividual model)
        # await self.store_in_elastic(final_individuals)
    
    async def _get_individual_details(self, irn: str, headers: dict) -> dict | None:
        """Get comprehensive individual data."""
        try:
            # Get basic individual details
            detail_url = f"{settings.FCA_API_BASE_URL}/Individuals/{irn}"
            detail_data = await self._api_call(detail_url, headers)
            
            if not detail_data or not detail_data.get("Data"):
                return None
            
            individual_data = detail_data["Data"][0]["Details"]
            
            # Get controlled functions
            cf_url = f"{settings.FCA_API_BASE_URL}/Individuals/{irn}/CF"
            cf_data = await self._api_call(cf_url, headers)
            
            controlled_functions = []
            if cf_data and cf_data.get("Data"):
                for cf_group in cf_data["Data"]:
                    # Current roles
                    if "Current" in cf_group:
                        for role_name, role_data in cf_group["Current"].items():
                            controlled_functions.append({
                                "role": role_name,
                                "firm_name": role_data.get("Firm Name", ""),
                                "status": "Current",
                                "effective_date": role_data.get("Effective Date", "")
                            })
                    
                    # Previous roles
                    if "Previous" in cf_group:
                        for role_name, role_data in cf_group["Previous"].items():
                            controlled_functions.append({
                                "role": role_name,
                                "firm_name": role_data.get("Firm Name", ""),
                                "status": "Previous",
                                "effective_date": role_data.get("Effective Date", ""),
                                "end_date": role_data.get("End Date", "")
                            })
            
            # Get disciplinary history
            disciplinary_url = f"{settings.FCA_API_BASE_URL}/Individuals/{irn}/DisciplinaryHistory"
            disciplinary_data = await self._api_call(disciplinary_url, headers)
            
            disciplinary_history = []
            if disciplinary_data and disciplinary_data.get("Data"):
                for action in disciplinary_data["Data"]:
                    disciplinary_history.append({
                        "action_type": action.get("TypeofAction", ""),
                        "enforcement_type": action.get("EnforcementType", ""),
                        "description": action.get("TypeofDescription", ""),
                        "effective_date": action.get("ActionEffectiveFrom", "")
                    })
            
            return {
                "irn": irn,
                "full_name": individual_data.get("Full Name", ""),
                "commonly_used_name": individual_data.get("Commonly Used Name", ""),
                "status": individual_data.get("Status", ""),
                "controlled_functions": controlled_functions,
                "disciplinary_history": disciplinary_history
            }
            
        except Exception as e:
            logger.warning(f"Failed to get details for individual {irn}: {e}")
            return None


class ElasticFCAProductsLoader(ElasticDataLoader):
    """Loader for FCA Collective Investment Schemes (Products) data."""
    
    def __init__(self, elastic_client: AsyncElasticsearch, index_name: str):
        super().__init__(elastic_client, index_name)
    
    async def _api_call(self, endpoint: str, headers: dict) -> dict | None:
        """Make FCA API call with proper status code handling."""
        try:
            response = await cached_limited_get(endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            status = data.get("Status", "")
            message = data.get("Message", "")
            
            # Success codes that contain data
            if status.startswith("FSR-API-") and any(keyword in message for keyword in [
                "Ok", "Found", "successful", "Success"
            ]):
                return data
            
            # Valid "not found" responses - these are OK, just no data available
            elif status.startswith("FSR-API-") and any(keyword in message for keyword in [
                "not found", "Not Found", "No search result found"
            ]):
                return {"Status": status, "Message": message, "Data": None}
            
            # Any other FSR-API response is valid (just log and return)
            elif status.startswith("FSR-API-"):
                logger.debug(f"API response for {endpoint}: {status} - {message}")
                return data
            
            else:
                logger.warning(f"Unexpected API response for {endpoint}: {status} - {message}")
                return None
                
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error {e.response.status_code} for {endpoint}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed API call to {endpoint}: {e}")
            return None
    
    async def load_investment_products(self, limit: int = 100):
        """Load Collective Investment Schemes data."""
        logger.info("Loading FCA Collective Investment Schemes...")
        
        headers = {
            "x-auth-email": settings.FCA_API_EMAIL,
            "x-auth-key": settings.FCA_API_KEY,
            "Content-Type": "application/json"
        }
        
        # Search for funds/products
        search_terms = ["fund", "investment", "trust", "scheme", "portfolio"]
        all_products = {}
        
        with self.progress_context() as progress:
            search_task = progress.add_task("Searching for investment products", total=len(search_terms))
            
            for term in search_terms:
                try:
                    search_url = f"{settings.FCA_API_BASE_URL}/Search"
                    search_params = {"q": term, "type": "fund"}
                    
                    response = await cached_limited_get(search_url, params=search_params, headers=headers)
                    response.raise_for_status()
                    search_data = response.json()
                    
                    if search_data.get("Status") == "FSR-API-04-01-00" and search_data.get("Data"):
                        for product_summary in search_data["Data"][:20]:
                            prn = product_summary.get("Reference Number")
                            if prn and prn not in all_products:
                                # Get detailed product information
                                product_detail = await self._get_product_details(prn, headers)
                                if product_detail:
                                    all_products[prn] = product_detail
                    
                    progress.update(search_task, advance=1)
                    
                except Exception as e:
                    logger.error(f"Failed to search for '{term}' products: {e}")
                    progress.update(search_task, advance=1)
                    continue
        
        final_products = list(all_products.values())[:limit]
        logger.info(f"Loaded {len(final_products)} investment products")
        
        # Store products (you'd need to create an FCAProduct model)
        # await self.store_in_elastic(final_products)
    
    async def _get_product_details(self, prn: str, headers: dict) -> dict | None:
        """Get comprehensive product data."""
        try:
            # Get basic product details
            detail_url = f"{settings.FCA_API_BASE_URL}/CIS/{prn}"
            detail_data = await self._api_call(detail_url, headers)
            
            if not detail_data or not detail_data.get("Data"):
                return None
            
            product_data = detail_data["Data"][0]
            
            # Get subfunds
            subfund_url = f"{settings.FCA_API_BASE_URL}/CIS/{prn}/Subfund"
            subfund_data = await self._api_call(subfund_url, headers)
            
            subfunds = []
            if subfund_data and subfund_data.get("Data"):
                for subfund in subfund_data["Data"]:
                    subfunds.append({
                        "name": subfund.get("Name", ""),
                        "type": subfund.get("Sub-Fund Type", "")
                    })
            
            # Get other names
            names_url = f"{settings.FCA_API_BASE_URL}/CIS/{prn}/Names"
            names_data = await self._api_call(names_url, headers)
            
            other_names = []
            if names_data and names_data.get("Data"):
                for name_info in names_data["Data"]:
                    other_names.append({
                        "name": name_info.get("Product Other Name", ""),
                        "effective_from": name_info.get("Effective From", ""),
                        "effective_to": name_info.get("Effective To", "")
                    })
            
            return {
                "prn": prn,
                "operator_name": product_data.get("Operator Name", ""),
                "product_type": product_data.get("Product Type", ""),
                "scheme_type": product_data.get("Scheme Type", ""),
                "status": product_data.get("Status", ""),
                "effective_date": product_data.get("Effective Date", ""),
                "cis_depositary_name": product_data.get("CIS Depositary Name", ""),
                "subfunds": subfunds,
                "other_names": other_names
            }
            
        except Exception as e:
            logger.warning(f"Failed to get details for product {prn}: {e}")
            return None


class ElasticFCAEnforcementLoader(ElasticDataLoader):
    """Loader for FCA Enforcement Notices."""
    
    def __init__(self, elastic_client: AsyncElasticsearch, index_name: str):
        super().__init__(elastic_client, index_name)
        
    async def load_enforcement_notices(self, from_date: str | None = None, to_date: str | None = None):
        """Load FCA Enforcement Notices within date range."""
        logger.info("Loading FCA Enforcement Notices...")
        
        # Sample enforcement data
        sample_notices = [
            {
                "notice_id": "2024-001",
                "notice_type": "Final Notice",
                "subject_name": "Example Financial Services Ltd",
                "publication_date": "2024-01-30T00:00:00Z",
                "action_taken": "Financial penalty of £500,000",
                "summary": "Failures in anti-money laundering controls",
                "content": "The FCA found significant weaknesses in the firm's AML procedures...",
                "fine_amount": 500000.00,
                "currency": "GBP"
            }
        ]
        
        notices = []
        for notice_data in sample_notices:
            notice = FCAEnforcementNotice(**notice_data)
            notices.append(notice)
            
        await self.store_in_elastic(notices)


async def load_data(
    es_client: AsyncElasticsearch,
    settings: FCAmcpSettings,
    source: str,
    from_date: str | None = None,
    to_date: str | None = None,
):
    """Load data from specified source into Elasticsearch.

    Args:
        es_client: Elasticsearch client
        settings: FCAmcpSettings instance
        source: Data source - "hansard", "parliamentary-questions", "handbook", "policy-documents", "consultation-papers", "firms-register", "individuals", "products", "enforcement-notices"
        from_date: Start date in YYYY-MM-DD format (optional)
        to_date: End date in YYYY-MM-DD format (optional)
    """
    # Legacy parliamentary data loaders (kept for transition)
    if source == "hansard":
        loader = ElasticHansardLoader(elastic_client=es_client, index_name=settings.HANSARD_CONTRIBUTIONS_INDEX)
        await loader.load_all_contributions(from_date, to_date)
    elif source == "parliamentary-questions":
        loader = ElasticParliamentaryQuestionLoader(
            elastic_client=es_client, index_name=settings.PARLIAMENTARY_QUESTIONS_INDEX
        )
        await loader.load_questions_for_date_range(from_date, to_date)
    
    # FCA data loaders
    elif source == "handbook":
        loader = ElasticFCAHandbookLoader(elastic_client=es_client, index_name=settings.FCA_HANDBOOK_INDEX)
        await loader.load_handbook_sections()
    elif source == "policy-documents":
        loader = ElasticFCAPolicyStatementLoader(elastic_client=es_client, index_name=settings.FCA_POLICY_STATEMENTS_INDEX)
        await loader.load_policy_statements(from_date, to_date)
    elif source == "consultation-papers":
        loader = ElasticFCAConsultationPaperLoader(elastic_client=es_client, index_name=settings.FCA_CONSULTATION_PAPERS_INDEX)
        await loader.load_consultation_papers(from_date, to_date)
    elif source == "firms-register":
        loader = ElasticFCAAuthorisedFirmsLoader(elastic_client=es_client, index_name=settings.FCA_AUTHORISED_FIRMS_INDEX)
        await loader.load_authorised_firms()
    elif source == "individuals":
        # Load individuals data by first getting firms, then their associated individuals
        firm_loader = ElasticFCAAuthorisedFirmsLoader(elastic_client=es_client, index_name=settings.FCA_AUTHORISED_FIRMS_INDEX)
        # Get list of FRNs from existing firm data or search
        # For now, use a sample set - in practice you'd query existing firm data
        sample_frns = ["123456", "789012", "345678"]  # This would come from existing firm data
        
        individual_loader = ElasticFCAIndividualsLoader(elastic_client=es_client, index_name=settings.FCA_INDIVIDUALS_INDEX)
        await individual_loader.load_individuals_from_firms(sample_frns)
    elif source == "products":
        loader = ElasticFCAProductsLoader(elastic_client=es_client, index_name=settings.FCA_PRODUCTS_INDEX)
        await loader.load_investment_products()
    elif source == "enforcement-notices":
        loader = ElasticFCAEnforcementLoader(elastic_client=es_client, index_name=settings.FCA_ENFORCEMENT_NOTICES_INDEX)
        await loader.load_enforcement_notices(from_date, to_date)
    else:
        raise ValueError(f"Unknown source: {source}. Supported sources: hansard, parliamentary-questions, handbook, policy-documents, consultation-papers, firms-register, individuals, products, enforcement-notices")
