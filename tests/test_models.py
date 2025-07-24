"""Unit tests for FCA MCP models."""

from fca_mcp.models import Contribution, FCAHandbookSection


def test_contribution_document_uri():
    """Test that Contribution model computes document URIs correctly."""
    # Test with ContributionExtId present
    contribution_with_ext_id = Contribution(
        DebateSectionExtId="debate-123",
        ContributionExtId="contrib-456",
        ContributionText="This is a contribution",
        OrderInDebateSection=1,
    )
    assert contribution_with_ext_id.document_uri == "debate_debate-123_contrib_contrib-456"

    # Test without ContributionExtId - should use hash
    contribution_without_ext_id = Contribution(
        DebateSectionExtId="debate-789",
        ContributionExtId=None,
        ContributionText="Another contribution",
        OrderInDebateSection=2,
    )
    # Should create a hash-based URI
    expected_hash = "debate_debate-789_contrib_"
    assert contribution_without_ext_id.document_uri.startswith(expected_hash)
    assert len(contribution_without_ext_id.document_uri) > len(expected_hash)

    # Test that same inputs produce same hash
    contribution_same_inputs = Contribution(
        DebateSectionExtId="debate-789",
        ContributionExtId=None,
        ContributionText="Another contribution",
        OrderInDebateSection=2,
    )
    assert contribution_without_ext_id.document_uri == contribution_same_inputs.document_uri

    # Test that different inputs produce different hashes
    contribution_different_text = Contribution(
        DebateSectionExtId="debate-789",
        ContributionExtId=None,
        ContributionText="Different contribution",
        OrderInDebateSection=2,
    )
    assert contribution_without_ext_id.document_uri != contribution_different_text.document_uri


def test_fca_handbook_section_document_uri():
    """Test that FCAHandbookSection model computes document URIs correctly."""
    handbook_section = FCAHandbookSection(
        section_id="PRIN_1_1",
        chapter="PRIN",
        section_number="1.1",
        title="The Principles for Businesses",
        content="A firm must conduct its business with integrity.",
        content_type="rule",
        is_current=True,
    )
    assert handbook_section.document_uri == "handbook_PRIN_1_1"


def test_fca_handbook_section_url():
    """Test that FCAHandbookSection model computes URLs correctly.""" 
    handbook_section = FCAHandbookSection(
        section_id="PRIN_1_1",
        chapter="PRIN", 
        section_number="1.1",
        title="The Principles for Businesses",
        content="A firm must conduct its business with integrity.",
        content_type="rule",
        is_current=True,
    )
    assert handbook_section.handbook_url == "https://www.handbook.fca.org.uk/handbook/PRIN_1_1"
