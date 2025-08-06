# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is the official Python client for Databricks SQL. It implements PEP 249 (DB API 2.0) and uses Apache Thrift for communication with Databricks clusters/SQL warehouses.

## Essential Development Commands

```bash
# Install dependencies
poetry install

# Install with PyArrow support (recommended)
poetry install --all-extras

# Run unit tests
poetry run python -m pytest tests/unit

# Run specific test
poetry run python -m pytest tests/unit/test_client.py::ClientTestSuite::test_method_name

# Code formatting (required before commits)
poetry run black src

# Type checking
poetry run mypy --install-types --non-interactive src

# Check formatting without changing files
poetry run black src --check
```

## High-Level Architecture

### Core Components

1. **Client Layer** (`src/databricks/sql/client.py`)
   - Main entry point implementing DB API 2.0
   - Handles connections, cursors, and query execution
   - Key classes: `Connection`, `Cursor`

2. **Backend Layer** (`src/databricks/sql/backend/`)
   - Thrift-based communication with Databricks
   - Handles protocol-level operations
   - Key files: `thrift_backend.py`, `databricks_client.py`
   - SEA (Streaming Execute API) support in `experimental/backend/sea_backend.py`

3. **Authentication** (`src/databricks/sql/auth/`)
   - Multiple auth methods: OAuth U2M/M2M, PAT, custom providers
   - Authentication flow abstraction
   - OAuth persistence support for token caching

4. **Data Transfer** (`src/databricks/sql/cloudfetch/`)
   - Cloud fetch for large results
   - Arrow format support for efficiency
   - Handles data pagination and streaming
   - Result set management in `result_set.py`

5. **Parameters** (`src/databricks/sql/parameters/`)
   - Native parameter support (v3.0.0+) - server-side parameterization
   - Inline parameters (legacy) - client-side interpolation
   - SQL injection prevention
   - Type mapping and conversion

6. **Telemetry** (`src/databricks/sql/telemetry/`)
   - Usage metrics and performance monitoring
   - Configurable batch processing and time-based flushing
   - Server-side flag integration

### Key Design Patterns

- **Result Sets**: Uses Arrow format by default for efficient data transfer
- **Error Handling**: Comprehensive retry logic with exponential backoff
- **Resource Management**: Context managers for proper cleanup
- **Type System**: Strong typing with MyPy throughout

## Testing Strategy

### Unit Tests (No Databricks account needed)
```bash
poetry run python -m pytest tests/unit
```

### E2E Tests (Requires Databricks account)
1. Set environment variables or create `test.env` file:
   ```bash
   export DATABRICKS_SERVER_HOSTNAME="****"
   export DATABRICKS_HTTP_PATH="/sql/1.0/endpoints/****"
   export DATABRICKS_TOKEN="dapi****"
   ```
2. Run: `poetry run python -m pytest tests/e2e`

Test organization:
- `tests/unit/` - Fast, isolated unit tests
- `tests/e2e/` - Integration tests against real Databricks
- Test files follow `test_*.py` naming convention
- Test suites: core, large queries, staging ingestion, retry logic

## Important Development Notes

1. **Dependency Management**: Always use Poetry, never pip directly
2. **Code Style**: Black formatter with 100-char line limit (PEP 8 with this exception)
3. **Type Annotations**: Required for all new code
4. **Thrift Files**: Generated code in `thrift_api/` - do not edit manually
5. **Parameter Security**: Always use native parameters, never string interpolation
6. **Arrow Support**: Optional but highly recommended for performance
7. **Python Support**: 3.8+ (up to 3.13)
8. **DCO**: Sign commits with Developer Certificate of Origin

## Common Development Tasks

### Adding a New Feature
1. Implement in appropriate module under `src/databricks/sql/`
2. Add unit tests in `tests/unit/`
3. Add integration tests in `tests/e2e/` if needed
4. Update type hints and ensure MyPy passes
5. Run Black formatter before committing

### Debugging Connection Issues
- Check auth configuration in `auth/` modules
- Review retry logic in `src/databricks/sql/utils.py`
- Enable debug logging for detailed trace

### Working with Thrift
- Protocol definitions in `src/databricks/sql/thrift_api/`
- Backend implementation in `backend/thrift_backend.py`
- Don't modify generated Thrift files directly

### Running Examples
Example scripts are in `examples/` directory:
- Basic query execution examples
- OAuth authentication patterns
- Parameter usage (native vs inline)
- Staging ingestion operations
- Custom credential providers