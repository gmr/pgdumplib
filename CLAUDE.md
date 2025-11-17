# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pgdumplib is a Python3 library for reading and writing PostgreSQL pg_dump files in the custom format (`-Fc`). The library parses the binary format of pg_dump files and provides APIs to:

- Load existing pg_dump files and iterate through their contents
- Create new pg_dump files programmatically
- Read table data with configurable data converters
- Handle BLOB data
- Manage dump entries with proper dependency resolution using topological sorting

## Core Architecture

### Main Components

- **`pgdumplib/dump.py`**: Contains the `Dump` class, which is the primary interface for loading, creating, and manipulating pg_dump files. Also includes `TableData` class for managing table data via temporary gzip-compressed files.

- **`pgdumplib/models.py`**: Defines the `Entry` dataclass representing individual entries in a dump's table of contents (TOC). Entries contain metadata, DDL, dependencies, and data offsets.

- **`pgdumplib/converters.py`**: Data converter classes that transform table data:
  - `DataConverter`: Base converter that only converts `\N` to None
  - `NoOpConverter`: Returns raw strings unchanged
  - `SmartDataConverter`: Attempts native Python type conversion (int, datetime, Decimal, IP addresses, UUID)

- **`pgdumplib/constants.py`**: Extensive constants defining PostgreSQL object types, dump format versions, sections (Pre-Data, Data, Post-Data), and mappings between them.

### Key Design Patterns

1. **Temporary File Management**: Table and blob data are stored in gzip-compressed temporary files that are automatically cleaned up when the Dump instance is released.

2. **Dependency Resolution**: Entries have dependencies tracked via `dump_id` references. The library uses `toposort` to ensure proper ordering when writing dumps.

3. **Binary Format Parsing**: The library implements the pg_dump custom format specification, handling magic bytes (`PGDMP`), versioning (supports versions 1.12.0-1.14.0), compression, and data blocks.

4. **Pluggable Converters**: Data conversion is abstracted, allowing users to provide custom converters when loading/creating dumps.

## Development Commands

### Environment Setup

```bash
python3 -m venv env
source env/bin/activate
pip install -e '.[dev]'
```

### Testing

The test suite requires PostgreSQL running and test fixtures generated:

```bash
# Local development with Docker Compose:
./bootstrap  # Starts Postgres, generates fixtures, creates test dumps

# After bootstrap, source the test environment:
. build/test-environment

# Run tests:
ci/test  # Runs pre-commit hooks, then coverage with unittest

# Or manually:
coverage run -m unittest discover tests
coverage report  # Must be >= 90% coverage
```

**Single test execution**:
```bash
python -m unittest tests.test_dump.DumpTestCase.test_specific_method
```

### Linting and Formatting

```bash
# Run all pre-commit hooks:
pre-commit run --all-files

# Ruff is configured in pyproject.toml:
# - Line length: 79 characters
# - Quote style: single quotes
# - Target: Python 3.12
```

### Test Fixtures

Test fixtures are created by the `bootstrap` script and `ci/generate-fixture-data.py`:
- `build/data/dump.not-compressed` - Uncompressed dump
- `build/data/dump.compressed` - Compressed dump (level 9)
- `build/data/dump.no-data` - Schema-only dump
- `build/data/dump.data-only` - Data-only dump
- `build/data/dump.inserts` - Dump using INSERT statements

Tests inherit from `EnvironmentVariableMixin` to load connection info from `build/test-environment`.

## Code Style Requirements

- **Coverage**: Pull requests require test coverage. The project enforces >= 90% coverage.
- **Line length**: 79 characters
- **Quotes**: Single quotes for strings
- **Type hints**: Use modern Python type hints (3.11+ syntax with `|` for unions)
- **Target version**: Python 3.11, 3.12, 3.13 are supported

## PostgreSQL Version Support

The library supports PostgreSQL 9-18 with dump format versions 1.12.0-1.16.0. Version mapping is defined in `constants.K_VERSION_MAP`.

- Format version 1.12.0: PostgreSQL 9.0-10.2 (separate BLOB entries)
- Format version 1.13.0: PostgreSQL 10.3-11.x (search_path behavior change)
- Format version 1.14.0: PostgreSQL 12-15 (table access methods)
- Format version 1.15.0: PostgreSQL 16 (compression algorithm in header)
- Format version 1.16.0: PostgreSQL 17-18 (BLOB METADATA entries, multiple BLOBS, relkind)

## CI/CD

GitHub Actions runs tests on:
- Python versions: 3.11, 3.12, 3.13
- PostgreSQL versions: 16, 17, 18
- All commits to all branches (except docs-only changes)
- Uploads coverage to Codecov
