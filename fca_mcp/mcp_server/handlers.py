from datetime import datetime
from typing import Any, Literal

from elasticsearch import AsyncElasticsearch


def build_date_range_filter(date_from: str | None, date_to: str | None, field: str = "SittingDate") -> dict | None:
    """Build a date range filter for Elasticsearch queries."""
    if not date_from and not date_to:
        return None

    date_range = {"range": {field: {}}}
    if date_from:
        date_range["range"][field]["gte"] = date_from
    if date_to:
        date_range["range"][field]["lte"] = date_to
    return date_range


def build_house_filter(house: str | None) -> dict | None:
    """Build a house filter for Elasticsearch queries."""
    if not house:
        return None
    return {"term": {"House.keyword": house}}


def add_filter_if_exists(filters: list, filter_dict: dict | None) -> None:
    """Add a filter to the list if it exists."""
    if filter_dict:
        filters.append(filter_dict)


def build_source_fields(includes: list[str], excludes: list[str] | None = None) -> dict:
    """Build _source field configuration for Elasticsearch queries."""
    source_config = {"includes": includes}
    if excludes:
        source_config["excludes"] = excludes
    return source_config


def build_semantic_query(query: str, field: str, boost: float = 1.0) -> dict:
    """Build a semantic search query for a field."""
    return {"semantic": {"field": field, "query": query, "boost": boost}}


async def search_debates(
    *,
    es_client: AsyncElasticsearch,
    index: str,
    query: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    house: str | None = None,
    max_results: int = 100,
) -> list[dict]:
    """
    Search debates for a given query, date range, and house.

    Either query or date range (or both) must be provided. House is optional.
    If only date_from is provided, returns debates from that date onwards.
    If only date_to is provided, returns debates up to and including that date.

    Returns a list of debate details (ID, title, date) ranked by relevancy.

    Args:
        query: Text to search for in debate titles (optional if date range is provided)
        date_from: Start date in format 'YYYY-MM-DD' (optional if query is provided)
        date_to: End date in format 'YYYY-MM-DD' (optional if query is provided)
        house: Filter by house (e.g., 'Commons', 'Lords'), optional
        max_results: Maximum number of results to return (default 100)

    Returns:
        List of debate details dictionaries

    Raises:
        ValueError: If neither query nor date range is provided
    """
    # Validate that at least one of query or date range is provided
    if not query and not date_from and not date_to:
        message = "At least one of 'query', 'date_from', or 'date_to' must be provided"
        raise ValueError(message)

    # Core filters that apply to all searches
    filters = [
        # Exclude debates with no attributions
        {"exists": {"field": "AttributedTo"}},
        {"bool": {"must_not": {"term": {"AttributedTo": ""}}}},
    ]

    # Add optional filters
    add_filter_if_exists(filters, build_date_range_filter(date_from, date_to))
    add_filter_if_exists(filters, build_house_filter(house))

    # Build query
    bool_query = {"filter": filters}
    if query:
        bool_query["must"] = [{"match": {"debate_parents.Title": {"query": query}}}]

    # Construct the complete query with aggregations for unique debates
    query_body = {
        "query": {
            "bool": bool_query,
        },
        "size": 0,
        "aggs": {
            "unique_debates": {
                "terms": {
                    "field": "DebateSectionExtId.keyword",
                    "size": max_results,
                    "min_doc_count": 2,
                    "order": {"max_score": "desc"},  # Order by relevance score
                },
                "aggs": {
                    "max_score": {"max": {"script": {"source": "_score"}}},
                    "debate_info": {
                        "top_hits": {
                            "_source": [
                                "DebateSection",
                                "SittingDate",
                                "DebateSectionExtId",
                                "debate_parents.ExternalId",
                                "debate_parents.Title",
                                "House",
                            ],
                            "size": 1,
                        }
                    },
                },
            }
        },
    }

    # Execute search
    response = await es_client.search(index=index, body=query_body)

    # Extract and format debate details
    debates = []
    for bucket in response["aggregations"]["unique_debates"]["buckets"]:
        source = bucket["debate_info"]["hits"]["hits"][0]["_source"]
        debates.append(
            {
                "debate_id": source["DebateSectionExtId"],
                "title": source["DebateSection"],
                "date": source["SittingDate"],
                "house": source["House"],
                "relevance_score": bucket["max_score"]["value"],
                "debate_parents": source.get("debate_parents", []),
            }
        )

    return debates


async def search_hansard_contributions(
    *,
    es_client: AsyncElasticsearch,
    index: str,
    query: str | None = None,
    memberId: int | None = None,
    dateFrom: str | None = None,
    dateTo: str | None = None,
    debateId: str | None = None,
    house: Literal["Commons", "Lords"] | None = None,
    maxResults: int = 100,
    min_score: float = 0.5,
) -> list[dict]:
    """
    Search Hansard for a given query, member ID, date range, debate ID, sort order, house, and maximum results.

    Args:
        query: Text to search for in debate titles (optional)
        memberId: Member ID (optional)
        dateFrom: Start date in format 'YYYY-MM-DD' (optional)
        dateTo: End date in format 'YYYY-MM-DD' (optional)
        debateId: Debate ID (optional)
        sortBy: Sort by (chronological|relevance) (optional)
        house: House (Commons|Lords) (optional)
        maxResults: Maximum number of results to return (default 100)

    Returns:
        List of Hansard details dictionaries

    Raises:
        ValueError: If neither query nor memberId is provided
    """

    # Fail if none of the parameters are provided
    if not query and not memberId and not dateFrom and not dateTo and not debateId and not house:
        msg = "At least one of 'query', 'memberId', 'dateFrom', 'dateTo', or 'debateId' or 'house' must be provided"
        raise ValueError(msg)

    # Construct the filter list
    filters = []

    # Add filters
    if memberId:
        filters.append({"term": {"MemberId": memberId}})
    if debateId:
        filters.append({"term": {"DebateSectionExtId.keyword": debateId}})

    add_filter_if_exists(filters, build_date_range_filter(dateFrom, dateTo))
    add_filter_if_exists(filters, build_house_filter(house))

    query_body = {
        "_source": build_source_fields(
            includes=[
                "contribution_url",
                "debate_url",
                "ContributionTextFull.text",
                "SittingDate",
                "OrderInDebateSection",
                "House",
                "MemberId",
                "MemberName",
                "debate_parents.ExternalId",
                "debate_parents.Title",
                "DebateSection",
            ],
            excludes=["ContributionTextFull.inference"],
        ),
        "query": {"bool": {"filter": filters}},
        "size": maxResults,
        "sort": [
            {"_score": {"order": "desc"}},
            {"SittingDate": {"order": "asc"}},
            {"debate_parents.ExternalId.keyword": {"order": "asc"}},
            {"OrderInDebateSection": {"order": "asc"}},
        ],
    }

    if query:
        # If a query is provided, then use a hybrid search to find the most relevant contributions
        query_body["min_score"] = min_score
        query_body["query"]["bool"]["must"] = [build_semantic_query(query, "ContributionTextFull", 1.0)]

    response = await es_client.search(index=index, body=query_body)

    results = []
    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        results.append(
            {
                "text": source["ContributionTextFull"]["text"],
                "date": source.get("SittingDate"),
                "house": source.get("House"),
                "member_id": source.get("MemberId"),
                "member_name": source.get("MemberName"),
                "relevance_score": hit["_score"],
                "debate_title": source["DebateSection"],
                "debate_url": source["debate_url"],
                "contribution_url": source["contribution_url"],
                "order_in_debate": source["OrderInDebateSection"],
                "debate_parents": source["debate_parents"],
            }
        )

    return results


async def search_parliamentary_questions(
    es_client: AsyncElasticsearch,
    index: str,
    query: str | None = None,
    dateFrom: str | None = None,
    dateTo: str | None = None,
    party: str | None = None,
    member_name: str | None = None,
    member_id: int | None = None,
) -> list[dict]:
    """
    Search Parliamentary Questions for a given query, date range, party, and member name.

    Args:
        es_client: Elasticsearch client
        query: Text to search for in parliamentary questions
        dateFrom: Start date in format 'YYYY-MM-DD' (optional)
        dateTo: End date in format 'YYYY-MM-DD' (optional)
        party: Filter by party (optional)
        member_name: Filter by member name (optional)
        member_id: Filter by member id (optional)
    """

    query_body = build_parliamentary_questions_query(query, 0.5, dateFrom, dateTo, party, member_name, member_id)
    response = await es_client.search(index=index, body=query_body)
    results = []
    for hit in response["hits"]["hits"]:
        results.append(parse_parliamentary_questions_hit(hit))
    return results


def build_parliamentary_questions_query(
    query: str | None,
    min_score: float,
    start_date: str | None,
    end_date: str | None,
    party: str | None,
    member_name: str | None,
    member_id: int | None,
) -> dict[str, Any]:
    """
    Build Elasticsearch query for searching questions.

    Args:
        query: Search query string
        min_score: Minimum relevance score
        start_date: Optional start date filter
        end_date: Optional end date filter
        party: Optional party name filter
        member_name: Optional member name filter
        member_id: Optional member id filter
    Returns:
        Elasticsearch query dictionary
    """
    filter_conditions: list[dict[str, Any]] = []

    # Add a filter to only include documents that have a valid dateTabled
    filter_conditions.append({"exists": {"field": "dateTabled"}})

    # Add date range filter if specified
    add_filter_if_exists(filter_conditions, build_date_range_filter(start_date, end_date, "dateTabled"))

    if party:
        filter_conditions.append({"term": {"askingMember.party.keyword": party}})

    if member_name:
        filter_conditions.append({"term": {"askingMember.name.keyword": member_name}})

    if member_id:
        filter_conditions.append({"term": {"askingMember.id": member_id}})

    base_query = {
        "min_score": min_score,
        "_source": build_source_fields(
            includes=[
                "uin",
                "questionText",
                "answerText",
                "askingMember",
                "answeringMember",
                "dateTabled",
                "dateAnswered",
            ],
            excludes=["questionText.inference", "answerText.inference"],
        ),
    }

    if query:
        base_query["query"] = {
            "bool": {
                "should": [
                    build_semantic_query(query, "questionText", 1.0),
                    build_semantic_query(query, "answerText", 0.8),
                ],
                "minimum_should_match": 1,
            }
        }
    else:
        base_query["query"] = {
            "bool": {
                "must": filter_conditions,
            }
        }

    return base_query


def parse_parliamentary_questions_hit(hit: dict[str, Any]) -> dict[str, Any]:
    """
    Parse a single Elasticsearch hit into the desired format.

    Args:
        hit: Raw Elasticsearch hit dictionary

    Returns:
        Formatted dictionary containing parsed hit data
    """
    source = hit.get("_source", {})

    def safe_get_text(field_value: Any) -> str:
        if isinstance(field_value, dict):
            return field_value.get("text", "")
        return str(field_value) if field_value is not None else ""

    def parse_date(date_str: str | None) -> str | None:
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str).isoformat()
        except (ValueError, TypeError):
            return None

    return {
        "uin": safe_get_text(source.get("uin")),
        "score": hit.get("_score"),
        "questionText": safe_get_text(source.get("questionText")),
        "answerText": safe_get_text(source.get("answerText")),
        "askingMember": source.get("askingMember"),
        "answeringMember": source.get("answeringMember"),
        "dateTabled": parse_date(source.get("dateTabled")),
        "dateAnswered": parse_date(source.get("dateAnswered")),
    }


# =============================================================================
# FCA-Specific Handler Functions
# =============================================================================

async def search_fca_handbook(
    *,
    es_client: AsyncElasticsearch,
    index: str,
    query: str,
    chapter: str | None = None,
    content_type: str | None = None,
    is_current: bool = True,
    limit: int = 10,
) -> list[dict]:
    """Search FCA Handbook for rules and guidance."""
    # Core filters
    filters = []
    
    if is_current:
        filters.append({"term": {"is_current": True}})
    
    if chapter:
        filters.append({"term": {"chapter.keyword": chapter}})
    
    if content_type:
        filters.append({"term": {"content_type.keyword": content_type}})

    # Build semantic search
    query_body = {
        "query": {
            "bool": {
                "must": [
                    build_semantic_query(query, "content", boost=2.0),
                    build_semantic_query(query, "title", boost=1.5),
                ],
                "filter": filters,
            }
        },
        "size": limit,
        "_source": build_source_fields(
            includes=[
                "section_id", "chapter", "section_number", "title", 
                "content", "content_type", "effective_date", "handbook_url"
            ],
            excludes=["title.inference", "content.inference"]
        )
    }

    response = await es_client.search(index=index, body=query_body)
    results = []
    for hit in response["hits"]["hits"]:
        cleaned_result = clean_semantic_fields(hit["_source"])
        results.append(cleaned_result)
    return results


def extract_semantic_text(field_value: Any) -> str:
    """Extract text from semantic_text field, handling both dict and string formats."""
    if isinstance(field_value, dict) and "text" in field_value:
        return field_value["text"]
    return str(field_value) if field_value is not None else ""


def clean_semantic_fields(result: dict) -> dict:
    """Clean semantic_text fields to return only text values."""
    cleaned = {}
    for key, value in result.items():
        if isinstance(value, dict) and "text" in value:
            cleaned[key] = value["text"]
        else:
            cleaned[key] = value
    return cleaned


async def search_policy_statements(
    *,
    es_client: AsyncElasticsearch,
    index: str,
    query: str,
    policy_area: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    ps_number: str | None = None,
    limit: int = 10,
) -> list[dict]:
    """Search FCA Policy Statements."""
    filters = []
    
    if policy_area:
        filters.append({"term": {"policy_area.keyword": policy_area}})
    
    if ps_number:
        filters.append({"term": {"ps_number.keyword": ps_number}})
    
    add_filter_if_exists(filters, build_date_range_filter(from_date, to_date, "publication_date"))

    query_body = {
        "query": {
            "bool": {
                "must": [
                    build_semantic_query(query, "content", boost=2.0),
                    build_semantic_query(query, "summary", boost=1.5),
                    build_semantic_query(query, "title", boost=1.0),
                ],
                "filter": filters,
            }
        },
        "size": limit,
        "_source": build_source_fields(
            includes=[
                "ps_number", "title", "publication_date", "summary", 
                "content", "policy_area", "fca_url"
            ],
            excludes=["title.inference", "summary.inference", "content.inference"]
        )
    }

    response = await es_client.search(index=index, body=query_body)
    results = []
    for hit in response["hits"]["hits"]:
        cleaned_result = clean_semantic_fields(hit["_source"])
        results.append(cleaned_result)
    return results


async def search_consultation_papers(
    *,
    es_client: AsyncElasticsearch,
    index: str,
    query: str,
    policy_area: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    cp_number: str | None = None,
    open_for_consultation: bool | None = None,
    limit: int = 10,
) -> list[dict]:
    """Search FCA Consultation Papers."""
    filters = []
    
    if policy_area:
        filters.append({"term": {"policy_area.keyword": policy_area}})
    
    if cp_number:
        filters.append({"term": {"cp_number.keyword": cp_number}})
    
    if open_for_consultation is not None:
        if open_for_consultation:
            # Consultation is still open (closes date is in the future)
            filters.append({"range": {"consultation_closes": {"gte": "now"}}})
        else:
            # Consultation is closed
            filters.append({"range": {"consultation_closes": {"lt": "now"}}})
    
    add_filter_if_exists(filters, build_date_range_filter(from_date, to_date, "publication_date"))

    query_body = {
        "query": {
            "bool": {
                "must": [
                    build_semantic_query(query, "content", boost=2.0),
                    build_semantic_query(query, "summary", boost=1.5),
                    build_semantic_query(query, "title", boost=1.0),
                ],
                "filter": filters,
            }
        },
        "size": limit,
        "_source": build_source_fields(
            includes=[
                "cp_number", "title", "publication_date", "consultation_closes",
                "summary", "content", "policy_area", "fca_url"
            ],
            excludes=["title.inference", "summary.inference", "content.inference"]
        )
    }

    response = await es_client.search(index=index, body=query_body)
    results = []
    for hit in response["hits"]["hits"]:
        cleaned_result = clean_semantic_fields(hit["_source"])
        results.append(cleaned_result)
    return results


async def search_authorised_firms(
    *,
    es_client: AsyncElasticsearch,
    index: str,
    query: str | None = None,
    firm_name: str | None = None,
    city: str | None = None,
    permissions: str | None = None,
    firm_status: str = "Authorised",
    limit: int = 10,
) -> list[dict]:
    """Search FCA Authorised Firms register."""
    filters = [{"term": {"firm_status.keyword": firm_status}}]
    
    if city:
        filters.append({"term": {"city.keyword": city}})
    
    if permissions:
        filters.append({"match": {"permissions": permissions}})

    must_clauses = []
    
    if query:
        must_clauses.extend([
            {"match": {"firm_name": {"query": query, "boost": 2.0}}},
            {"match": {"trading_names": {"query": query, "boost": 1.5}}},
        ])
    
    if firm_name:
        must_clauses.append({"match": {"firm_name": {"query": firm_name, "boost": 2.0}}})

    query_body = {
        "query": {
            "bool": {
                "must": must_clauses if must_clauses else [{"match_all": {}}],
                "filter": filters,
            }
        },
        "size": limit,
        "_source": {
            "includes": [
                "firm_reference_number", "firm_name", "trading_names", 
                "firm_status", "permissions", "address_line_1", "city", 
                "postcode", "country", "telephone", "register_url"
            ]
        }
    }

    response = await es_client.search(index=index, body=query_body)
    return [hit["_source"] for hit in response["hits"]["hits"]]


async def get_firm_by_frn(
    *,
    es_client: AsyncElasticsearch,
    index: str,
    firm_reference_number: str,
) -> dict | None:
    """Get firm details by Firm Reference Number."""
    query_body = {
        "query": {"term": {"firm_reference_number.keyword": firm_reference_number}},
        "size": 1,
    }

    response = await es_client.search(index=index, body=query_body)
    hits = response["hits"]["hits"]
    return hits[0]["_source"] if hits else None


async def search_enforcement_notices(
    *,
    es_client: AsyncElasticsearch,
    index: str,
    query: str,
    notice_type: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    min_fine_amount: float | None = None,
    subject_name: str | None = None,
    limit: int = 10,
) -> list[dict]:
    """Search FCA Enforcement Notices."""
    filters = []
    
    if notice_type:
        filters.append({"term": {"notice_type.keyword": notice_type}})
    
    if min_fine_amount:
        filters.append({"range": {"fine_amount": {"gte": min_fine_amount}}})
    
    if subject_name:
        filters.append({"match": {"subject_name": subject_name}})
    
    add_filter_if_exists(filters, build_date_range_filter(from_date, to_date, "publication_date"))

    query_body = {
        "query": {
            "bool": {
                "must": [
                    build_semantic_query(query, "content", boost=2.0),
                    build_semantic_query(query, "summary", boost=1.5),
                    {"match": {"subject_name": {"query": query, "boost": 1.0}}},
                ],
                "filter": filters,
            }
        },
        "size": limit,
        "_source": build_source_fields(
            includes=[
                "notice_id", "notice_type", "subject_name", "publication_date",
                "action_taken", "summary", "content", "fine_amount", "currency"
            ],
            excludes=["summary.inference", "content.inference"]
        )
    }

    response = await es_client.search(index=index, body=query_body)
    results = []
    for hit in response["hits"]["hits"]:
        cleaned_result = clean_semantic_fields(hit["_source"])
        results.append(cleaned_result)
    return results


async def search_guidance_documents(
    *,
    es_client: AsyncElasticsearch,
    index: str,
    query: str,
    document_type: str | None = None,
    topic_area: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 10,
) -> list[dict]:
    """Search FCA Guidance Documents."""
    filters = []
    
    if document_type:
        filters.append({"term": {"document_type.keyword": document_type}})
    
    if topic_area:
        filters.append({"term": {"topic_area.keyword": topic_area}})
    
    add_filter_if_exists(filters, build_date_range_filter(from_date, to_date, "publication_date"))

    query_body = {
        "query": {
            "bool": {
                "must": [
                    build_semantic_query(query, "content", boost=2.0),
                    build_semantic_query(query, "title", boost=1.5),
                ],
                "filter": filters,
            }
        },
        "size": limit,
        "_source": build_source_fields(
            includes=[
                "title", "document_type", "topic_area", "publication_date",
                "content", "summary", "source_url"
            ],
            excludes=["title.inference", "content.inference", "summary.inference"]
        )
    }

    response = await es_client.search(index=index, body=query_body)
    results = []
    for hit in response["hits"]["hits"]:
        cleaned_result = clean_semantic_fields(hit["_source"])
        results.append(cleaned_result)
    return results


async def get_regulatory_updates(
    *,
    es_client: AsyncElasticsearch,
    topic_area: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    document_types: list[str] | None = None,
    limit: int = 20,
) -> list[dict]:
    """Get recent regulatory updates across all FCA document types."""
    # Search across multiple indices
    indices = [
        "fca_mcp_policy_statements",
        "fca_mcp_consultation_papers", 
        "fca_mcp_enforcement_notices",
        "fca_mcp_guidance_documents"
    ]
    
    filters = []
    
    if topic_area:
        filters.append({
            "bool": {
                "should": [
                    {"term": {"policy_area.keyword": topic_area}},
                    {"term": {"topic_area.keyword": topic_area}},
                ]
            }
        })
    
    if document_types:
        filters.append({
            "bool": {
                "should": [
                    {"terms": {"notice_type.keyword": document_types}},
                    {"terms": {"document_type.keyword": document_types}},
                ]
            }
        })
    
    add_filter_if_exists(filters, build_date_range_filter(from_date, to_date, "publication_date"))

    query_body = {
        "query": {
            "bool": {
                "filter": filters if filters else [{"match_all": {}}],
            }
        },
        "sort": [{"publication_date": {"order": "desc"}}],
        "size": limit,
    }

    # Search across all indices
    all_results = []
    for index in indices:
        try:
            response = await es_client.search(index=index, body=query_body)
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                source["document_index"] = index.replace("fca_mcp_", "")
                all_results.append(source)
        except Exception:
            # Index might not exist, continue with others
            continue
    
    # Sort all results by publication date and return top results
    all_results.sort(key=lambda x: x.get("publication_date", ""), reverse=True)
    return all_results[:limit]
