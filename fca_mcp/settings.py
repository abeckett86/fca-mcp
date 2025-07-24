import logging
import os
from functools import lru_cache

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

logger = logging.getLogger(__name__)


@lru_cache
def get_ssm_parameter(parameter_name: str, region: str = "eu-west-2") -> str:
    """Fetch a parameter from AWS Systems Manager Parameter Store."""
    try:
        ssm = boto3.client("ssm", region_name=region)
        response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        return response["Parameter"]["Value"]
    except (ClientError, BotoCoreError) as e:
        logger.warning("Could not fetch SSM parameter %s: %s", parameter_name, e)
        return ""


def get_environment_or_ssm(env_var_name: str, ssm_path: str | None = None, default: str = "") -> str:
    """Get value from environment variable or fall back to SSM parameter."""
    env_value = os.environ.get(env_var_name)
    if env_value:
        return env_value

    if ssm_path and os.environ.get("AWS_REGION"):
        return get_ssm_parameter(ssm_path, os.environ.get("AWS_REGION"))

    return default


class FCAmcpSettings(BaseSettings):
    """Configuration settings for FCA MCP application with environment-based loading."""

    APP_NAME: str
    AWS_ACCOUNT_ID: str | None = None
    AWS_REGION: str = "eu-west-2"
    ENVIRONMENT: str = "local"

    # Use SSM for sensitive parameters in AWS environments
    @property
    def SENTRY_DSN(self) -> str | None:
        return get_environment_or_ssm("SENTRY_DSN", f"/{self._get_project_name()}/env_secrets/SENTRY_DSN")

    @property
    def AZURE_OPENAI_API_KEY(self) -> str:
        return get_environment_or_ssm(
            "AZURE_OPENAI_API_KEY", f"/{self._get_project_name()}/env_secrets/AZURE_OPENAI_API_KEY"
        )

    @property
    def AZURE_OPENAI_ENDPOINT(self) -> str:
        return get_environment_or_ssm(
            "AZURE_OPENAI_ENDPOINT", f"/{self._get_project_name()}/env_secrets/AZURE_OPENAI_ENDPOINT"
        )

    @property
    def AZURE_OPENAI_RESOURCE_NAME(self) -> str:
        return get_environment_or_ssm(
            "AZURE_OPENAI_RESOURCE_NAME", f"/{self._get_project_name()}/env_secrets/AZURE_OPENAI_RESOURCE_NAME"
        )

    @property
    def AZURE_OPENAI_EMBEDDING_MODEL(self) -> str:
        return get_environment_or_ssm(
            "AZURE_OPENAI_EMBEDDING_MODEL", f"/{self._get_project_name()}/env_secrets/AZURE_OPENAI_EMBEDDING_MODEL"
        )

    @property
    def AZURE_OPENAI_API_VERSION(self) -> str:
        return get_environment_or_ssm(
            "AZURE_OPENAI_API_VERSION", f"/{self._get_project_name()}/env_secrets/AZURE_OPENAI_API_VERSION", "preview"
        )

    @property
    def AZURE_OPENAI_EMBEDDING_DEPLOYMENT(self) -> str:
        return get_environment_or_ssm(
            "AZURE_OPENAI_EMBEDDING_DEPLOYMENT", f"/{self._get_project_name()}/env_secrets/AZURE_OPENAI_EMBEDDING_DEPLOYMENT",
            default="text-embedding-3-large"
        )

    # Azure OpenAI settings (configured via properties below)

    # FCA API Configuration
    @property
    def FCA_API_KEY(self) -> str:
        return get_environment_or_ssm(
            "FCA_API_KEY", f"/{self._get_project_name()}/env_secrets/FCA_API_KEY"
        )

    @property
    def FCA_API_EMAIL(self) -> str:
        return get_environment_or_ssm(
            "FCA_API_EMAIL", f"/{self._get_project_name()}/env_secrets/FCA_API_EMAIL"  
        )

    @property
    def FCA_API_BASE_URL(self) -> str:
        return get_environment_or_ssm(
            "FCA_API_BASE_URL", f"/{self._get_project_name()}/env_secrets/FCA_API_BASE_URL",
            default="https://register.fca.org.uk/services/V0.1"
        )

    # Elasticsearch connection settings
    @property
    def ELASTICSEARCH_CLOUD_ID(self) -> str | None:
        return get_environment_or_ssm(
            "ELASTICSEARCH_CLOUD_ID", f"/{self._get_project_name()}/env_secrets/ELASTICSEARCH_CLOUD_ID"
        )

    @property
    def ELASTICSEARCH_API_KEY(self) -> str | None:
        return get_environment_or_ssm(
            "ELASTICSEARCH_API_KEY", f"/{self._get_project_name()}/env_secrets/ELASTICSEARCH_API_KEY"
        )

    @property
    def ELASTICSEARCH_HOST(self) -> str | None:
        return get_environment_or_ssm(
            "ELASTICSEARCH_HOST", f"/{self._get_project_name()}/env_secrets/ELASTICSEARCH_HOST", "localhost"
        )

    @property
    def ELASTICSEARCH_PORT(self) -> int:
        port_str = get_environment_or_ssm(
            "ELASTICSEARCH_PORT", f"/{self._get_project_name()}/env_secrets/ELASTICSEARCH_PORT", "9200"
        )
        return int(port_str) if port_str.isdigit() else 9200

    ELASTICSEARCH_SCHEME: str = "http"

    AUTH_PROVIDER_PUBLIC_KEY: str | None = None
    DISABLE_AUTH_SIGNATURE_VERIFICATION: bool = ENVIRONMENT == "local"

    def _get_project_name(self) -> str:
        """Get the project name from environment or use default."""
        return os.environ.get("PROJECT_NAME", "i-dot-ai-dev-parliament-mcp")

    # Set to 0 for single-node cluster
    ELASTICSEARCH_INDEX_PATTERN: str = "fca_mcp_*"
    ELASTICSEARCH_NUMBER_OF_REPLICAS: int = 0

    EMBEDDING_INFERENCE_ENDPOINT_NAME: str = "openai-embedding-inference"
    EMBEDDING_DIMENSIONS: int = 1024

    # Chunking settings
    # See https://www.elastic.co/search-labs/blog/elasticsearch-chunking-inference-api-endpoints
    CHUNK_SIZE: int = 300
    SENTENCE_OVERLAP: int = 1
    CHUNK_STRATEGY: str = "sentence"

    # Legacy Parliamentary indices (keep for transition)
    PARLIAMENTARY_QUESTIONS_INDEX: str = "fca_mcp_parliamentary_questions"
    HANSARD_CONTRIBUTIONS_INDEX: str = "fca_mcp_hansard_contributions"

    # FCA-specific indices
    FCA_HANDBOOK_INDEX: str = "fca_mcp_handbook"
    FCA_POLICY_STATEMENTS_INDEX: str = "fca_mcp_policy_statements"
    FCA_CONSULTATION_PAPERS_INDEX: str = "fca_mcp_consultation_papers"
    FCA_GUIDANCE_DOCUMENTS_INDEX: str = "fca_mcp_guidance_documents"
    FCA_ENFORCEMENT_NOTICES_INDEX: str = "fca_mcp_enforcement_notices"
    FCA_AUTHORISED_FIRMS_INDEX: str = "fca_mcp_authorised_firms"
    FCA_INDIVIDUALS_INDEX: str = "fca_mcp_individuals"
    FCA_PRODUCTS_INDEX: str = "fca_mcp_products"

    # MCP settings
    MCP_HOST: str = "0.0.0.0"  # nosec B104 - Binding to all interfaces is intentional for containerized deployment
    MCP_PORT: int = 8080

    # The MCP server can be accessed at /{MCP_ROOT_PATH}/mcp
    MCP_ROOT_PATH: str = "/"

    # Rate limiting settings for parliament.uk API.
    HTTP_MAX_RATE_PER_SECOND: float = 10

    # FCA-specific API settings
    FCA_HANDBOOK_API_BASE_URL: str = "https://www.handbook.fca.org.uk/api"
    FCA_REGISTER_API_BASE_URL: str = "https://register.fca.org.uk/services/V0.1"
    FCA_WEBSITE_BASE_URL: str = "https://www.fca.org.uk"

    # ========================================
    # Complete FCA Register API Endpoints Map
    # ========================================

    # FIRM ENDPOINTS (16 endpoints)
    FCA_FIRM_DETAILS: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>
    FCA_FIRM_NAMES: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"    # /V0.1/Firm/<FRN>/Names
    FCA_FIRM_ADDRESS: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/Address
    FCA_FIRM_CONTROLLED_FUNCTIONS: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/CF
    FCA_FIRM_INDIVIDUALS: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/Individuals
    FCA_FIRM_PERMISSIONS: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/Permissions
    FCA_FIRM_REQUIREMENTS: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/Requirements
    FCA_FIRM_REQUIREMENT_INVESTMENT_TYPES: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/Requirements/<ReqRef>/InvestmentTypes
    FCA_FIRM_REGULATORS: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/Regulators
    FCA_FIRM_PASSPORTS: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/Passports
    FCA_FIRM_PASSPORT_PERMISSIONS: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/Passports/<Country>/Permission
    FCA_FIRM_WAIVERS: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/Waivers
    FCA_FIRM_EXCLUSIONS: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/Exclusions
    FCA_FIRM_DISCIPLINARY_HISTORY: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/DisciplinaryHistory
    FCA_FIRM_APPOINTED_REPRESENTATIVES: str = f"{FCA_REGISTER_API_BASE_URL}/Firm"  # /V0.1/Firm/<FRN>/AR

    # INDIVIDUAL ENDPOINTS (4 endpoints)
    FCA_INDIVIDUAL_DETAILS: str = f"{FCA_REGISTER_API_BASE_URL}/Individuals"  # /V0.1/Individuals/<IRN>
    FCA_INDIVIDUAL_CONTROLLED_FUNCTIONS: str = f"{FCA_REGISTER_API_BASE_URL}/Individuals"  # /V0.1/Individuals/<IRN>/CF
    FCA_INDIVIDUAL_DISCIPLINARY_HISTORY: str = f"{FCA_REGISTER_API_BASE_URL}/Individuals"  # /V0.1/Individuals/<IRN>/DisciplinaryHistory

    # PRODUCT ENDPOINTS (4 endpoints)
    FCA_PRODUCT_DETAILS: str = f"{FCA_REGISTER_API_BASE_URL}/CIS"  # /V0.1/CIS/<PRN>
    FCA_PRODUCT_SUBFUNDS: str = f"{FCA_REGISTER_API_BASE_URL}/CIS"  # /V0.1/CIS/<PRN>/Subfund
    FCA_PRODUCT_OTHER_NAMES: str = f"{FCA_REGISTER_API_BASE_URL}/CIS"  # /V0.1/CIS/<PRN>/Names

    # SEARCH ENDPOINTS (2 endpoints)
    FCA_COMMON_SEARCH: str = f"{FCA_REGISTER_API_BASE_URL}/Search"  # /V0.1/Search?q=<query>&type=<type>
    FCA_REGULATED_MARKET_SEARCH: str = f"{FCA_REGISTER_API_BASE_URL}/CommonSearch"  # /V0.1/CommonSearch?q=RM

    # FCA data ingestion settings
    FCA_RATE_LIMIT_REQUESTS: int = 60  # Requests per minute
    FCA_RATE_LIMIT_WINDOW: int = 60    # Window in seconds

    # FCA API Authentication Headers
    def get_fca_auth_headers(self) -> dict[str, str]:
        """Get FCA API authentication headers."""
        return {
            "x-auth-email": self.FCA_API_EMAIL,
            "x-auth-key": self.FCA_API_KEY,
            "Content-Type": "application/json"
        }

    # Load environment variables from .env file in local environment
    # from pydantic_settings import SettingsConfigDict
    if ENVIRONMENT == "local":
        model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = FCAmcpSettings()
