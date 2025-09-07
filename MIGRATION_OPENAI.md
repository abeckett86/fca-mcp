# Migration from Azure OpenAI to Direct OpenAI API

## Summary
This document outlines the complete migration from Azure OpenAI to the direct OpenAI API for the FCA MCP Server project.

## Changes Made

### 1. Settings Configuration (`fca_mcp/settings.py`)
**Removed:**
- `AZURE_OPENAI_API_KEY`
- `AZURE_OPENAI_ENDPOINT`
- `AZURE_OPENAI_RESOURCE_NAME`
- `AZURE_OPENAI_EMBEDDING_MODEL`
- `AZURE_OPENAI_API_VERSION`
- `AZURE_OPENAI_EMBEDDING_DEPLOYMENT`

**Added:**
- `OPENAI_API_KEY` - Direct OpenAI API key
- `OPENAI_EMBEDDING_MODEL` - Model identifier (default: `text-embedding-3-large`)
- `OPENAI_API_BASE_URL` - Optional custom endpoint (default: `https://api.openai.com/v1`)

### 2. Elasticsearch Integration (`fca_mcp/elasticsearch_helpers.py`)
**Changed inference endpoint configuration:**
- Service: `"azureopenai"` → `"openai"`
- Service settings:
  - Removed: `resource_name`, `deployment_id`, `api_version`
  - Updated: `api_key` → Uses `OPENAI_API_KEY`
  - Added: `model_id` → Uses `OPENAI_EMBEDDING_MODEL`
  - Kept: `dimensions` (1024) and chunking settings

### 3. Environment Configuration (`.env.example`)
**Before:**
```
AZURE_OPENAI_API_KEY=XXX
AZURE_OPENAI_ENDPOINT=https://[[my-resource-name]].openai.azure.com/
AZURE_OPENAI_RESOURCE_NAME=[[my-resource-name]]
AZURE_OPENAI_EMBEDDING_MODEL=text-embedding-3-large
AZURE_OPENAI_API_VERSION=2025-03-01-preview
```

**After:**
```
OPENAI_API_KEY=your-openai-api-key-here
OPENAI_EMBEDDING_MODEL=text-embedding-3-large
# OPENAI_API_BASE_URL=https://api.openai.com/v1  # Optional
```

### 4. Documentation Updates (`README.md`)
- Updated all references from "Azure OpenAI" to "OpenAI"
- Changed prerequisite requirements
- Updated setup instructions

## Migration Steps for Users

### 1. Update Environment Variables
Create or update your `.env` file:
```bash
cp .env.example .env
```

Then edit `.env` and set:
```
OPENAI_API_KEY=sk-your-openai-api-key-here
OPENAI_EMBEDDING_MODEL=text-embedding-3-large
```

### 2. Clear Existing Elasticsearch Data (if needed)
If you have existing data with Azure OpenAI embeddings:
```bash
# Delete old indices and inference endpoints
fca-mcp delete-elasticsearch

# Reinitialize with OpenAI
fca-mcp init-elasticsearch

# Reload your data
fca-mcp load-data handbook
fca-mcp load-data policy-documents
# ... etc
```

### 3. Restart Services
```bash
# If using Docker
docker-compose down
docker-compose up -d

# If running locally
make run_mcp_server
```

## Benefits of Migration

1. **Simpler Configuration**: Fewer environment variables needed
2. **Direct API Access**: No Azure wrapper, potentially lower latency
3. **Same Model Quality**: Using the same `text-embedding-3-large` model
4. **Cost Transparency**: Direct OpenAI pricing without Azure markup
5. **Global Availability**: Not tied to Azure region restrictions

## Compatibility Notes

- The same embedding model (`text-embedding-3-large`) is used, ensuring compatibility
- Embedding dimensions remain at 1024
- All semantic search functionality works identically
- No changes needed to API endpoints or tool usage

## Rollback Instructions

If you need to rollback to Azure OpenAI:
1. Restore the original `settings.py` and `elasticsearch_helpers.py` files
2. Update `.env` with Azure OpenAI credentials
3. Restart services

## Support

For any issues with the migration, please:
1. Check that your OpenAI API key is valid
2. Ensure you have sufficient OpenAI API credits
3. Verify Elasticsearch is running and accessible
4. Review logs with `docker-compose logs mcp-server`
