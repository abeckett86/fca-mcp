import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Literal

import sentry_sdk
from mcp.server.fastmcp.server import FastMCP
from pydantic import Field

from fca_mcp.elasticsearch_helpers import get_async_es_client
from fca_mcp.settings import settings
from fca_mcp.data_loaders import cached_limited_get

from . import handlers
from .utils import log_tool_call, sanitize_params

logger = logging.getLogger(__name__)


@asynccontextmanager
async def mcp_lifespan(_server: FastMCP) -> AsyncGenerator[dict]:
    """Manage application lifecycle with type-safe context"""
    # Initialize on startup
    async with get_async_es_client(settings) as es_client:
        yield {
            "es_client": es_client,
        }


mcp_server = FastMCP(name="FCA MCP Server", stateless_http=True, lifespan=mcp_lifespan)

# init Sentry if configured
if settings.SENTRY_DSN and settings.ENVIRONMENT in ["dev", "preprod", "prod"] and settings.SENTRY_DSN != "placeholder":
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        traces_sample_rate=1.0,
        profiles_sample_rate=1.0,
    )


@mcp_server.tool("search_fca_handbook")
@log_tool_call
async def search_fca_handbook(
    query: str = Field(..., description="Search text to find in FCA Handbook rules and guidance"),
    chapter: str | None = Field(None, description="Specific handbook chapter (e.g., 'PRIN', 'COBS', 'SYSC')"),
    content_type: str | None = Field(None, description="Filter by content type: 'rule', 'guidance', 'schedule'"),
    is_current: bool = Field(True, description="Whether to search only current rules (default True)"),
    limit: int = Field(10, description="Maximum number of results to return (max 50)"),
) -> Any:
    """
    Search the FCA Handbook for rules, guidance, and other regulatory content.
    
    The FCA Handbook contains the rules and guidance issued by the FCA,
    organized by different sourcebooks (PRIN, COBS, SYSC, etc.).
    
    Examples:
    - search_fca_handbook(query="consumer duty") - Search for Consumer Duty rules
    - search_fca_handbook(query="client money", chapter="CASS") - Search client money rules in CASS
    - search_fca_handbook(query="senior managers", content_type="rule") - Find SM&CR rules only
    """
    params = sanitize_params(**locals())
    
    async with get_async_es_client(settings) as es_client:
        try:
            results = await handlers.search_fca_handbook(
                es_client=es_client,
                index=settings.FCA_HANDBOOK_INDEX,
                query=params["query"],
                chapter=params.get("chapter"),
                content_type=params.get("content_type"),
                is_current=params.get("is_current", True),
                limit=min(params.get("limit", 10), 50),
            )
            return {"results": results}
        except Exception as e:
            logger.error(f"Error searching FCA Handbook: {e}")
            raise


@mcp_server.tool("search_policy_statements")
@log_tool_call
async def search_policy_statements(
    query: str = Field(..., description="Search text to find in FCA Policy Statements"),
    policy_area: str | None = Field(None, description="Filter by policy area (e.g., 'Consumer Protection', 'Operational Resilience')"),
    from_date: str | None = Field(None, description="Start date for publication date filter (YYYY-MM-DD)"),
    to_date: str | None = Field(None, description="End date for publication date filter (YYYY-MM-DD)"),
    ps_number: str | None = Field(None, description="Specific PS number to search for (e.g., 'PS24/1')"),
    limit: int = Field(10, description="Maximum number of results to return (max 50)"),
) -> Any:
    """
    Search FCA Policy Statements (PS) which contain final policy decisions and statements.
    
    Policy Statements set out the FCA's final policy positions on regulatory matters,
    often following consultation papers.
    
    Examples:
    - search_policy_statements(query="consumer duty implementation") - Find Consumer Duty PS
    - search_policy_statements(policy_area="Operational Resilience") - Find all operational resilience policies
    - search_policy_statements(ps_number="PS24/1") - Get specific policy statement
    """
    params = sanitize_params(**locals())
    
    async with get_async_es_client(settings) as es_client:
        try:
            results = await handlers.search_policy_statements(
                es_client=es_client,
                index=settings.FCA_POLICY_STATEMENTS_INDEX,
                query=params["query"],
                policy_area=params.get("policy_area"),
                from_date=params.get("from_date"),
                to_date=params.get("to_date"),
                ps_number=params.get("ps_number"),
                limit=min(params.get("limit", 10), 50),
            )
            return {"results": results}
        except Exception as e:
            logger.error(f"Error searching Policy Statements: {e}")
            raise


@mcp_server.tool("search_consultation_papers")
@log_tool_call
async def search_consultation_papers(
    query: str = Field(..., description="Search text to find in FCA Consultation Papers"),
    policy_area: str | None = Field(None, description="Filter by policy area"),
    from_date: str | None = Field(None, description="Start date for publication date filter (YYYY-MM-DD)"),
    to_date: str | None = Field(None, description="End date for publication date filter (YYYY-MM-DD)"),
    cp_number: str | None = Field(None, description="Specific CP number to search for (e.g., 'CP24/1')"),
    open_for_consultation: bool | None = Field(None, description="Filter by whether consultation is still open"),
    limit: int = Field(10, description="Maximum number of results to return (max 50)"),
) -> Any:
    """
    Search FCA Consultation Papers (CP) which contain regulatory proposals seeking stakeholder feedback.
    
    Consultation Papers set out the FCA's proposed policy changes and seek public feedback
    before finalizing rules and guidance.
    
    Examples:
    - search_consultation_papers(query="crypto asset regulation") - Find crypto consultation papers
    - search_consultation_papers(open_for_consultation=True) - Find currently open consultations
    - search_consultation_papers(cp_number="CP24/1") - Get specific consultation paper
    """
    params = sanitize_params(**locals())
    
    async with get_async_es_client(settings) as es_client:
        try:
            results = await handlers.search_consultation_papers(
                es_client=es_client,
                index=settings.FCA_CONSULTATION_PAPERS_INDEX,
                query=params["query"],
                policy_area=params.get("policy_area"),
                from_date=params.get("from_date"),
                to_date=params.get("to_date"),
                cp_number=params.get("cp_number"),
                open_for_consultation=params.get("open_for_consultation"),
                limit=min(params.get("limit", 10), 50),
            )
            return {"results": results}
        except Exception as e:
            logger.error(f"Error searching Consultation Papers: {e}")
            raise


@mcp_server.tool("search_authorised_firms")
@log_tool_call
async def search_authorised_firms(
    query: str | None = Field(None, description="Search text to find in firm names or details"),
    firm_name: str | None = Field(None, description="Specific firm name to search for"),
    city: str | None = Field(None, description="Filter by city/location"),
    permissions: str | None = Field(None, description="Filter by permissions (e.g., 'Managing investments')"),
    firm_status: str = Field("Authorised", description="Firm status filter (default: 'Authorised')"),
    limit: int = Field(10, description="Maximum number of results to return (max 50)"),
) -> Any:
    """
    Search the FCA register of authorised firms and individuals.
    
    The Financial Services Register contains details of firms and individuals
    authorised by the FCA, including their permissions and status.
    
    Examples:
    - search_authorised_firms(query="investment management") - Search for investment managers
    - search_authorised_firms(firm_name="Barclays") - Find Barclays entities
    - search_authorised_firms(city="London", permissions="Managing investments") - London investment managers
    """
    params = sanitize_params(**locals())
    
    async with get_async_es_client(settings) as es_client:
        try:
            results = await handlers.search_authorised_firms(
                es_client=es_client,
                index=settings.FCA_AUTHORISED_FIRMS_INDEX,
                query=params.get("query"),
                firm_name=params.get("firm_name"),
                city=params.get("city"),
                permissions=params.get("permissions"),
                firm_status=params.get("firm_status", "Authorised"),
                limit=min(params.get("limit", 10), 50),
            )
            return {"results": results}
        except Exception as e:
            logger.error(f"Error searching authorised firms: {e}")
            raise


@mcp_server.tool("get_firm_details")
@log_tool_call
async def get_firm_details(
    firm_reference_number: str = Field(..., description="FCA Firm Reference Number (FRN)"),
) -> Any:
    """
    Get comprehensive details about a specific authorised firm using its FRN.
    
    Returns full details including permissions, address, contact information,
    and regulatory status for a specific firm.
    
    Examples:
    - get_firm_details(firm_reference_number="123456") - Get full details for firm 123456
    """
    params = sanitize_params(**locals())
    
    async with get_async_es_client(settings) as es_client:
        try:
            result = await handlers.get_firm_by_frn(
                es_client=es_client,
                index=settings.FCA_AUTHORISED_FIRMS_INDEX,
                firm_reference_number=params["firm_reference_number"],
            )
            return {"result": result}
        except Exception as e:
            logger.error(f"Error getting firm details: {e}")
            raise


@mcp_server.tool("search_enforcement_notices")
@log_tool_call
async def search_enforcement_notices(
    query: str = Field(..., description="Search text to find in enforcement notices and decisions"),
    notice_type: str | None = Field(None, description="Filter by notice type (e.g., 'Final Notice', 'Decision Notice')"),
    from_date: str | None = Field(None, description="Start date for publication date filter (YYYY-MM-DD)"),
    to_date: str | None = Field(None, description="End date for publication date filter (YYYY-MM-DD)"),
    min_fine_amount: float | None = Field(None, description="Minimum fine amount filter"),
    subject_name: str | None = Field(None, description="Filter by subject firm or individual name"),
    limit: int = Field(10, description="Maximum number of results to return (max 50)"),
) -> Any:
    """
    Search FCA enforcement notices including fines, sanctions, and disciplinary actions.
    
    Enforcement notices include Final Notices, Decision Notices, and other regulatory
    actions taken by the FCA against firms and individuals.
    
    Examples:
    - search_enforcement_notices(query="market manipulation") - Find market manipulation cases
    - search_enforcement_notices(min_fine_amount=1000000) - Find fines over £1m
    - search_enforcement_notices(notice_type="Final Notice") - Find final enforcement decisions
    """
    params = sanitize_params(**locals())
    
    async with get_async_es_client(settings) as es_client:
        try:
            results = await handlers.search_enforcement_notices(
                es_client=es_client,
                index=settings.FCA_ENFORCEMENT_NOTICES_INDEX,
                query=params["query"],
                notice_type=params.get("notice_type"),
                from_date=params.get("from_date"),
                to_date=params.get("to_date"),
                min_fine_amount=params.get("min_fine_amount"),
                subject_name=params.get("subject_name"),
                limit=min(params.get("limit", 10), 50),
            )
            return {"results": results}
        except Exception as e:
            logger.error(f"Error searching enforcement notices: {e}")
            raise


@mcp_server.tool("search_guidance_documents")
@log_tool_call
async def search_guidance_documents(
    query: str = Field(..., description="Search text to find in FCA guidance documents"),
    document_type: str | None = Field(None, description="Filter by document type (e.g., 'Technical Standard', 'Guidance')"),
    topic_area: str | None = Field(None, description="Filter by regulatory topic area"),
    from_date: str | None = Field(None, description="Start date for publication date filter (YYYY-MM-DD)"),
    to_date: str | None = Field(None, description="End date for publication date filter (YYYY-MM-DD)"),
    limit: int = Field(10, description="Maximum number of results to return (max 50)"),
) -> Any:
    """
    Search FCA guidance documents, technical standards, and other regulatory publications.
    
    Includes various forms of regulatory guidance, technical standards, and other
    documents that help interpret and implement FCA rules.
    
    Examples:
    - search_guidance_documents(query="operational resilience") - Find operational resilience guidance
    - search_guidance_documents(document_type="Technical Standard") - Find technical standards
    - search_guidance_documents(topic_area="Consumer Protection") - Find consumer protection guidance
    """
    params = sanitize_params(**locals())
    
    async with get_async_es_client(settings) as es_client:
        try:
            results = await handlers.search_guidance_documents(
                es_client=es_client,
                index=settings.FCA_GUIDANCE_DOCUMENTS_INDEX,
                query=params["query"],
                document_type=params.get("document_type"),
                topic_area=params.get("topic_area"),
                from_date=params.get("from_date"),
                to_date=params.get("to_date"),
                limit=min(params.get("limit", 10), 50),
            )
            return {"results": results}
        except Exception as e:
            logger.error(f"Error searching Guidance Documents: {e}")
            raise


@mcp_server.tool("get_regulatory_updates")
@log_tool_call
async def get_regulatory_updates(
    from_date: str | None = Field(None, description="Start date for updates (YYYY-MM-DD)"),
    to_date: str | None = Field(None, description="End date for updates (YYYY-MM-DD)"),
    update_type: str | None = Field(None, description="Type of update: 'policy', 'consultation', 'handbook', 'enforcement'"),
    limit: int = Field(20, description="Maximum number of updates to return (max 100)"),
) -> Any:
    """
    Get recent FCA regulatory updates and changes across all document types.
    
    This provides a unified view of recent regulatory activity including
    new policy statements, consultation papers, handbook updates, and enforcement actions.
    
    Examples:
    - get_regulatory_updates() - Get latest regulatory updates
    - get_regulatory_updates(update_type="enforcement") - Get recent enforcement actions only
    - get_regulatory_updates(from_date="2024-01-01") - Get updates since start of year
    """
    params = sanitize_params(**locals())
    
    # For now, return a placeholder - would need to implement cross-index aggregation
    return {
        "updates": [],
        "message": "Regulatory updates feature in development - use specific search tools for now"
    }


@mcp_server.tool("get_firm_address")
@log_tool_call
async def get_firm_address(
    firm_reference_number: str = Field(..., description="FCA Firm Reference Number (FRN)"),
) -> Any:
    """
    Get detailed address information for a specific FCA authorised firm.
    
    Returns comprehensive address details including business address,
    contact information, and any complaints handling addresses.
    
    Examples:
    - get_firm_address(firm_reference_number="615820") - Get address for specific firm
    """
    params = sanitize_params(**locals())
    
    headers = {
        "x-auth-email": settings.FCA_API_EMAIL,
        "x-auth-key": settings.FCA_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        endpoint = f"{settings.FCA_API_BASE_URL}/Firm/{params['firm_reference_number']}/Address"
        response = await cached_limited_get(endpoint, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get("Status", "").startswith("FSR-API-") and "Found" in data.get("Message", ""):
            return {"address_data": data.get("Data", [])}
        else:
            return {"address_data": [], "message": f"No address data found for FRN {params['firm_reference_number']}"}
            
    except Exception as e:
        logger.error(f"Error getting firm address: {e}")
        raise


@mcp_server.tool("get_firm_permissions")
@log_tool_call
async def get_firm_permissions(
    firm_reference_number: str = Field(..., description="FCA Firm Reference Number (FRN)"),
) -> Any:
    """
    Get detailed permissions and regulatory activities for a specific FCA authorised firm.
    
    Returns the firm's regulated activities, permissions, and any limitations
    or restrictions on their business.
    
    Examples:
    - get_firm_permissions(firm_reference_number="615820") - Get permissions for specific firm
    """
    params = sanitize_params(**locals())
    
    headers = {
        "x-auth-email": settings.FCA_API_EMAIL,
        "x-auth-key": settings.FCA_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        endpoint = f"{settings.FCA_API_BASE_URL}/Firm/{params['firm_reference_number']}/Permissions"
        response = await cached_limited_get(endpoint, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get("Status", "").startswith("FSR-API-"):
            return {"permissions_data": data.get("Data", {}), "status": data.get("Message", "")}
        else:
            return {"permissions_data": {}, "message": f"No permissions data found for FRN {params['firm_reference_number']}"}
            
    except Exception as e:
        logger.error(f"Error getting firm permissions: {e}")
        raise


@mcp_server.tool("get_firm_individuals")
@log_tool_call
async def get_firm_individuals(
    firm_reference_number: str = Field(..., description="FCA Firm Reference Number (FRN)"),
) -> Any:
    """
    Get individuals associated with a specific FCA authorised firm.
    
    Returns key personnel, approved persons, and senior managers
    associated with the firm.
    
    Examples:
    - get_firm_individuals(firm_reference_number="615820") - Get individuals for specific firm
    """
    params = sanitize_params(**locals())
    
    headers = {
        "x-auth-email": settings.FCA_API_EMAIL,
        "x-auth-key": settings.FCA_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        endpoint = f"{settings.FCA_API_BASE_URL}/Firm/{params['firm_reference_number']}/Individuals"
        response = await cached_limited_get(endpoint, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get("Status", "").startswith("FSR-API-") and "found" in data.get("Message", "").lower():
            return {"individuals_data": data.get("Data", [])}
        else:
            return {"individuals_data": [], "message": f"No individuals data found for FRN {params['firm_reference_number']}"}
            
    except Exception as e:
        logger.error(f"Error getting firm individuals: {e}")
        raise


@mcp_server.tool("get_firm_disciplinary_history")
@log_tool_call
async def get_firm_disciplinary_history(
    firm_reference_number: str = Field(..., description="FCA Firm Reference Number (FRN)"),
) -> Any:
    """
    Get disciplinary history and enforcement actions for a specific FCA authorised firm.
    
    Returns any past or current disciplinary actions, fines, restrictions,
    or other regulatory enforcement measures against the firm.
    
    Examples:
    - get_firm_disciplinary_history(firm_reference_number="615820") - Get disciplinary history for specific firm
    """
    params = sanitize_params(**locals())
    
    headers = {
        "x-auth-email": settings.FCA_API_EMAIL,
        "x-auth-key": settings.FCA_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        endpoint = f"{settings.FCA_API_BASE_URL}/Firm/{params['firm_reference_number']}/DisciplinaryHistory"
        response = await cached_limited_get(endpoint, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get("Status", "").startswith("FSR-API-"):
            return {"disciplinary_data": data.get("Data", []), "status": data.get("Message", "")}
        else:
            return {"disciplinary_data": [], "message": f"No disciplinary history found for FRN {params['firm_reference_number']}"}
            
    except Exception as e:
        logger.error(f"Error getting firm disciplinary history: {e}")
        raise


@mcp_server.tool("search_individuals")
@log_tool_call
async def search_individuals(
    query: str = Field(..., description="Search term for individual names"),
    limit: int = Field(10, description="Maximum number of results to return (max 50)"),
) -> Any:
    """
    Search for individuals in the FCA register by name.
    
    Returns individuals matching the search criteria including their
    Individual Reference Numbers (IRN) and current status.
    
    Examples:
    - search_individuals(query="John Smith") - Search for individuals named John Smith
    - search_individuals(query="Davies") - Search by surname
    """
    params = sanitize_params(**locals())
    
    headers = {
        "x-auth-email": settings.FCA_API_EMAIL,
        "x-auth-key": settings.FCA_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        endpoint = f"{settings.FCA_API_BASE_URL}/Search"
        search_params = {"q": params["query"], "type": "individual"}
        
        response = await cached_limited_get(endpoint, params=search_params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get("Status") == "FSR-API-04-01-00" and data.get("Data"):
            return {"individuals": data["Data"][:params.get("limit", 10)]}
        else:
            return {"individuals": [], "message": f"No individuals found matching '{params['query']}'"}
            
    except Exception as e:
        logger.error(f"Error searching individuals: {e}")
        raise


@mcp_server.tool("get_individual_details")
@log_tool_call
async def get_individual_details(
    individual_reference_number: str = Field(..., description="FCA Individual Reference Number (IRN)"),
) -> Any:
    """
    Get detailed information for a specific individual in the FCA register.
    
    Returns personal details, current and previous roles, controlled functions,
    and any disciplinary history for the individual.
    
    Examples:
    - get_individual_details(individual_reference_number="JOB01749") - Get details for specific individual
    """
    params = sanitize_params(**locals())
    
    headers = {
        "x-auth-email": settings.FCA_API_EMAIL,
        "x-auth-key": settings.FCA_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        # Get basic individual details
        endpoint = f"{settings.FCA_API_BASE_URL}/Individuals/{params['individual_reference_number']}"
        response = await cached_limited_get(endpoint, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get("Status", "").startswith("FSR-API-") and "found" in data.get("Message", "").lower():
            individual_data = data.get("Data", [])
            
            # Get controlled functions
            cf_endpoint = f"{settings.FCA_API_BASE_URL}/Individuals/{params['individual_reference_number']}/CF"
            try:
                cf_response = await cached_limited_get(cf_endpoint, headers=headers)
                cf_response.raise_for_status()
                cf_data = cf_response.json()
                controlled_functions = cf_data.get("Data", []) if cf_data.get("Status", "").startswith("FSR-API-") else []
            except:
                controlled_functions = []
            
            # Get disciplinary history
            disc_endpoint = f"{settings.FCA_API_BASE_URL}/Individuals/{params['individual_reference_number']}/DisciplinaryHistory"
            try:
                disc_response = await cached_limited_get(disc_endpoint, headers=headers)
                disc_response.raise_for_status()
                disc_data = disc_response.json()
                disciplinary_history = disc_data.get("Data", []) if disc_data.get("Status", "").startswith("FSR-API-") else []
            except:
                disciplinary_history = []
            
            return {
                "individual_data": individual_data,
                "controlled_functions": controlled_functions,
                "disciplinary_history": disciplinary_history
            }
        else:
            return {"individual_data": [], "message": f"No individual found with IRN {params['individual_reference_number']}"}
             
    except Exception as e:
        logger.error(f"Error getting individual details: {e}")
        raise


@mcp_server.tool("search_products")
@log_tool_call
async def search_products(
    query: str = Field(..., description="Search term for Collective Investment Schemes (CIS) / products"),
    limit: int = Field(10, description="Maximum number of results to return (max 50)"),
) -> Any:
    """
    Search for Collective Investment Schemes (CIS) and investment products in the FCA register.
    
    Returns products matching the search criteria including their
    Product Reference Numbers (PRN) and current status.
    
    Examples:
    - search_products(query="ABC Fund") - Search for ABC Fund
    - search_products(query="investment trust") - Search for investment trusts
    """
    params = sanitize_params(**locals())
    
    headers = {
        "x-auth-email": settings.FCA_API_EMAIL,
        "x-auth-key": settings.FCA_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        endpoint = f"{settings.FCA_API_BASE_URL}/Search"
        search_params = {"q": params["query"], "type": "fund"}
        
        response = await cached_limited_get(endpoint, params=search_params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get("Status") == "FSR-API-04-01-00" and data.get("Data"):
            return {"products": data["Data"][:params.get("limit", 10)]}
        else:
            return {"products": [], "message": f"No products found matching '{params['query']}'"}
            
    except Exception as e:
        logger.error(f"Error searching products: {e}")
        raise


@mcp_server.tool("get_product_details")
@log_tool_call
async def get_product_details(
    product_reference_number: str = Field(..., description="FCA Product Reference Number (PRN) for CIS"),
) -> Any:
    """
    Get detailed information for a specific Collective Investment Scheme (CIS) product.
    
    Returns product details, operator information, depositary details,
    subfunds, and other names associated with the product.
    
    Examples:
    - get_product_details(product_reference_number="767821") - Get details for specific product
    """
    params = sanitize_params(**locals())
    
    headers = {
        "x-auth-email": settings.FCA_API_EMAIL,
        "x-auth-key": settings.FCA_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        # Get basic product details
        endpoint = f"{settings.FCA_API_BASE_URL}/CIS/{params['product_reference_number']}"
        response = await cached_limited_get(endpoint, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get("Status", "").startswith("FSR-API-") and "Found" in data.get("Message", ""):
            product_data = data.get("Data", [])
            
            # Get subfunds
            subfund_endpoint = f"{settings.FCA_API_BASE_URL}/CIS/{params['product_reference_number']}/Subfund"
            try:
                subfund_response = await cached_limited_get(subfund_endpoint, headers=headers)
                subfund_response.raise_for_status()  
                subfund_data = subfund_response.json()
                subfunds = subfund_data.get("Data", []) if subfund_data.get("Status", "").startswith("FSR-API-") else []
            except:
                subfunds = []
            
            # Get other names
            names_endpoint = f"{settings.FCA_API_BASE_URL}/CIS/{params['product_reference_number']}/Names"
            try:
                names_response = await cached_limited_get(names_endpoint, headers=headers)
                names_response.raise_for_status()
                names_data = names_response.json()
                other_names = names_data.get("Data", []) if names_data.get("Status", "").startswith("FSR-API-") else []
            except:
                other_names = []
            
            return {
                "product_data": product_data,
                "subfunds": subfunds,
                "other_names": other_names
            }
        else:
            return {"product_data": [], "message": f"No product found with PRN {params['product_reference_number']}"}
            
    except Exception as e:
        logger.error(f"Error getting product details: {e}")
        raise
