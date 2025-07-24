import asyncio
import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from fca_mcp.cli import configure_logging, load_data
from fca_mcp.elasticsearch_helpers import get_async_es_client
from fca_mcp.settings import FCAmcpSettings, settings

# Configure logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)
configure_logging(level=log_level)


async def main(settings: FCAmcpSettings, from_date_str: str | None = None, to_date_str: str | None = None) -> None:
    """Main ingestion function that processes all FCA data sources."""

    async with get_async_es_client(settings) as es_client:
        # Load FCA Handbook (no date range needed)
        logger.info("Ingesting FCA Handbook data...")
        await load_data(
            es_client=es_client,
            settings=settings,
            source="handbook",
        )
        logger.info("FCA Handbook data ingestion complete.")

        # Load Policy Statements (with date range if provided)
        logger.info("Ingesting FCA Policy Statements...")
        await load_data(
            es_client=es_client,
            settings=settings,
            source="policy-documents",
            from_date=from_date_str,
            to_date=to_date_str,
        )
        logger.info("FCA Policy Statements ingestion complete.")

        # Load Consultation Papers (with date range if provided)
        logger.info("Ingesting FCA Consultation Papers...")
        await load_data(
            es_client=es_client,
            settings=settings,
            source="consultation-papers",
            from_date=from_date_str,
            to_date=to_date_str,
        )
        logger.info("FCA Consultation Papers ingestion complete.")

        # Load Authorised Firms register (no date range needed)
        logger.info("Ingesting FCA Authorised Firms register...")
        await load_data(
            es_client=es_client,
            settings=settings,
            source="firms-register",
        )
        logger.info("FCA Authorised Firms register ingestion complete.")

        # Load Enforcement Notices (with date range if provided)
        logger.info("Ingesting FCA Enforcement Notices...")
        await load_data(
            es_client=es_client,
            settings=settings,
            source="enforcement-notices",
            from_date=from_date_str,
            to_date=to_date_str,
        )
        logger.info("FCA Enforcement Notices ingestion complete.")


def handler(event: dict, _: Any) -> None:
    """
    AWS Lambda handler function.

    This function is the entry point for the Lambda execution.
    It triggers the daily data ingestion for the FCA MCP.

    Args:
        event (dict): Lambda event. Should be in the format:
            {
                "from_date": "2024-10-10",  # Optional
                "to_date": "2024-10-12",  # Optional
            }
        context (dict): Lambda context.

    Returns:
        None
    """
    logger.info("Starting daily data ingestion...")

    try:
        utc_now = datetime.now(UTC)

        if "to_date" in event:
            to_date_str = event["to_date"]
        else:
            logger.info("No to_date provided, using default of today")
            to_date_str = utc_now.strftime("%Y-%m-%d")

        if "from_date" in event:
            from_date_str = event["from_date"]
        else:
            logger.info("No from_date provided, using default of 2 days ago")
            from_date_str = (utc_now - timedelta(days=2)).strftime("%Y-%m-%d")

        logger.info("Ingesting data from %s to %s", from_date_str, to_date_str)

        asyncio.run(main(settings, from_date_str, to_date_str))
        logger.info("Daily data ingestion finished successfully.")

    except Exception:
        logger.exception("An error occurred during data ingestion")
        raise
