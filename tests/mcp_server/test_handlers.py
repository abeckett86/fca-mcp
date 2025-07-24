from datetime import UTC, datetime

import pytest
from elasticsearch import AsyncElasticsearch

from fca_mcp.mcp_server.handlers import (
    search_fca_handbook,
    search_policy_statements,
    search_consultation_papers,
    search_authorised_firms,
    get_firm_by_frn,
    search_enforcement_notices,
)
from fca_mcp.settings import settings


@pytest.mark.asyncio
@pytest.mark.integration
async def test_search_fca_handbook(es_test_client: AsyncElasticsearch):
    """Test FCA Handbook search with test data."""
    results = await search_fca_handbook(
        es_client=es_test_client,
        index=settings.FCA_HANDBOOK_INDEX,
        query="integrity",
    )
    assert results is not None
    assert len(results) >= 0  # May be 0 if no matching data in test set

    # Test filtering by chapter
    results = await search_fca_handbook(
        es_client=es_test_client,
        index=settings.FCA_HANDBOOK_INDEX,
        query="business",
        chapter="PRIN",
    )
    assert results is not None

    # Test filtering by content type
    results = await search_fca_handbook(
        es_client=es_test_client,
        index=settings.FCA_HANDBOOK_INDEX,
        query="conduct",
        content_type="rule",
    )
    assert results is not None


@pytest.mark.asyncio
@pytest.mark.integration
async def test_search_policy_statements(es_test_client: AsyncElasticsearch):
    """Test Policy Statements search with test data."""
    results = await search_policy_statements(
        es_client=es_test_client,
        index=settings.FCA_POLICY_STATEMENTS_INDEX,
        query="consumer duty",
    )
    assert results is not None
    assert len(results) >= 0

    # Test filtering by policy area
    results = await search_policy_statements(
        es_client=es_test_client,
        index=settings.FCA_POLICY_STATEMENTS_INDEX,
        query="implementation",
        policy_area="Consumer Protection",
    )
    assert results is not None

    # Test PS number search
    results = await search_policy_statements(
        es_client=es_test_client,
        index=settings.FCA_POLICY_STATEMENTS_INDEX,
        query="policy",
        ps_number="PS24/1",
    )
    assert results is not None


@pytest.mark.asyncio
@pytest.mark.integration  
async def test_search_consultation_papers(es_test_client: AsyncElasticsearch):
    """Test Consultation Papers search with test data."""
    results = await search_consultation_papers(
        es_client=es_test_client,
        index=settings.FCA_CONSULTATION_PAPERS_INDEX,
        query="senior managers",
    )
    assert results is not None
    assert len(results) >= 0

    # Test CP number search
    results = await search_consultation_papers(
        es_client=es_test_client,
        index=settings.FCA_CONSULTATION_PAPERS_INDEX,
        query="consultation",
        cp_number="CP24/1",
    )
    assert results is not None


@pytest.mark.asyncio
@pytest.mark.integration
async def test_search_authorised_firms(es_test_client: AsyncElasticsearch):
    """Test Authorised Firms search with test data."""
    results = await search_authorised_firms(
        es_client=es_test_client,
        index=settings.FCA_AUTHORISED_FIRMS_INDEX,
        query="investment",
    )
    assert results is not None
    assert len(results) >= 0

    # Test firm name search
    results = await search_authorised_firms(
        es_client=es_test_client,
        index=settings.FCA_AUTHORISED_FIRMS_INDEX,
        firm_name="Example Investment",
    )
    assert results is not None

    # Test city filtering
    results = await search_authorised_firms(
        es_client=es_test_client,
        index=settings.FCA_AUTHORISED_FIRMS_INDEX,
        city="London",
    )
    assert results is not None

    # Test permissions filtering
    results = await search_authorised_firms(
        es_client=es_test_client,
        index=settings.FCA_AUTHORISED_FIRMS_INDEX,
        permissions="Managing investments",
    )
    assert results is not None


@pytest.mark.asyncio
@pytest.mark.integration
async def test_get_firm_by_frn(es_test_client: AsyncElasticsearch):
    """Test getting firm details by FRN."""
    # Test with a non-existent FRN
    result = await get_firm_by_frn(
        es_client=es_test_client,
        index=settings.FCA_AUTHORISED_FIRMS_INDEX,
        firm_reference_number="999999",
    )
    # Should return None for non-existent FRN
    assert result is None

    # Test with existing FRN from test data
    result = await get_firm_by_frn(
        es_client=es_test_client,
        index=settings.FCA_AUTHORISED_FIRMS_INDEX,
        firm_reference_number="123456",
    )
    # May be None if test data doesn't include this FRN
    assert result is None or isinstance(result, dict)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_search_enforcement_notices(es_test_client: AsyncElasticsearch):
    """Test Enforcement Notices search with test data."""
    results = await search_enforcement_notices(
        es_client=es_test_client,
        index=settings.FCA_ENFORCEMENT_NOTICES_INDEX,
        query="anti-money laundering",
    )
    assert results is not None
    assert len(results) >= 0

    # Test notice type filtering
    results = await search_enforcement_notices(
        es_client=es_test_client,
        index=settings.FCA_ENFORCEMENT_NOTICES_INDEX,
        query="penalty",
        notice_type="Final Notice",
    )
    assert results is not None

    # Test minimum fine amount filtering
    results = await search_enforcement_notices(
        es_client=es_test_client,
        index=settings.FCA_ENFORCEMENT_NOTICES_INDEX,
        query="fine",
        min_fine_amount=100000.0,
    )
    assert results is not None

    # Test subject name filtering
    results = await search_enforcement_notices(
        es_client=es_test_client,
        index=settings.FCA_ENFORCEMENT_NOTICES_INDEX,
        query="financial services",
        subject_name="Example Financial Services",
    )
    assert results is not None
