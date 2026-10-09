# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Yuki is a data analysis management toolkit for high energy physics, serving as the "Data Integration Thought Entity for the Chern Project". It's a Flask web application with Celery task queue for distributed job execution, designed for scientific workflow management in physics research.

## Architecture

### Core Components

1. **Kernel Layer** (`Yuki/kernel/`): Job management abstraction
   - `vjob.py`: Virtual Job abstract base class with factory pattern for automatic subclass selection
   - `container_job.py`: Container-based job execution
   - `image_job.py`: Image-based job execution
   - `vworkflow.py`: Virtual workflow management
   - `reana_workflow.py`: REANA workflow integration
   - `native_workflow.py`: Native/local workflow support

2. **Server Layer** (`Yuki/server/`): Flask web server with Celery
   - `app.py`: Flask application setup and configuration
   - `config.py`: Configuration management
   - `tasks.py`: Celery task definitions
   - `routes/`: API endpoints (upload, execution, status, runner, workflow, transfer)

3. **Storage Organization**: Data stored in `~/.Yuki/Storage/` with project/impression hierarchy

### Key Design Patterns
- Factory pattern for job type selection
- Blueprint-based Flask route organization
- Celery for asynchronous task execution
- REANA client integration for scientific workflows

## Development Commands

### Installation and Setup
```bash
# Install in development mode
pip install -e .

# Build package
python -m build

# Install from built package
pip install dist/yuki-*.whl
```

### Server Management
```bash
# Start server (Flask + Celery on port 3315)
yuki server start

# Check server status
yuki server status

# Stop server
yuki server stop  # or Ctrl-C
```

### Docker Development
```bash
# Start dev environment with hot-reload
# Source code is mounted as a volume; RabbitMQ starts inside the container
docker compose up

# Optionally develop against a local CelebiChrono checkout instead of PyPI
CELEBI_DIR=../CelebiChrono docker compose up

# Build images locally
docker/scripts/build.sh dev            # yuki:dev
docker/scripts/build.sh prod           # yuki:<version> + yuki:latest (version from pyproject.toml)
docker/scripts/build.sh prod --tar     # also exports yuki-<version>.tar via docker save
docker/scripts/build.sh prod --nightly # yuki-nightly:0.0.<date>-1 naming
```

### Testing
```bash
# Run all tests
python -m unittest UnitTest.test_server -v

# Run specific test class
python -m unittest UnitTest.test_server.TestYukiServer -v

# Run specific test method
python -m unittest UnitTest.test_server.TestYukiServer.test_upload_file_post -v

# Using pytest
pytest UnitTest/test_server.py -v

# Using test runner script
python UnitTest/run_tests.py
```

### Linting and CI/CD
```bash
# Pylint (as configured in CI)
pylint --disable="fixme,too-many-ancestors,broad-exception-raised,broad-exception-caught,duplicate-code,import-outside-toplevel" $(git ls-files '*.py')
```

## Configuration

### Dependencies (from pyproject.toml)
- Core: `click`, `colored`, `python-daemon`, `ipython`
- Web: `flask`, `celery`
- Data: `PyYAML>=5.1`, `reana-client`, `pillow`

### Flask Configuration
- Port: 3315
- Max upload size: 1GB
- Celery broker: RabbitMQ (amqp://localhost)
- Template directory: `Yuki/templates/`

## Testing Strategy

### Test Organization (`UnitTest/`)
- **TestYukiServer**: Flask routes and endpoints
- **TestCeleryTasks**: Celery background tasks
- **TestUtilityFunctions**: Utility and helper functions

### Mocking Approach
Tests use extensive mocking to isolate units:
- File system operations (`os.path`, `os.listdir`)
- Flask file uploads and responses
- Celery task execution
- VJob, VWorkflow, VContainer classes
- External API calls (REANA)

## Workflow Integration

### REANA Support
- Integration via `reana-client` for scientific workflow management
- `reana_workflow.py` provides REANA-specific workflow handling

### Job Types
- Virtual Jobs (abstract base)
- Container Jobs (Docker-based execution)
- Image Jobs (pre-built image execution)

## Development Notes

### Absolute Imports
The project uses absolute imports within the package. Always import from `Yuki` package root.

### Storage Structure
```
~/.Yuki/Storage/
  ├── projects/
  │   └── [project_name]/
  │       └── impressions/
  │           └── [impression_id]/
  │               ├── inputs/
  │               ├── outputs/
  │               └── logs/
```

Note: as of the Docker consolidation (2026-08), containers run as non-root user `yuki`, so in-container storage is `/home/yuki/.Yuki` (previously `/root/.Yuki`). Docker setups mount the host `~/.Yuki` there; under rootless Docker `yuki docker run` runs as root and uses `/root/.Yuki` instead.

### Port Configuration
- Flask server runs on port 3315
- Celery uses RabbitMQ broker on localhost

### Docker Setup (`docker/`)
- **Dockerfile**: Multi-stage — `base` (system deps, RabbitMQ, non-root `yuki` user) → `dev` (editable install) and `prod` (wheel install, default stage). Python deps resolve from `pyproject.toml`.
- **docker-compose.yml**: Dev environment for macOS (`docker compose up`); bind-mounts host `~/.Yuki` (override via `YUKIDIR`) and optionally a local CelebiChrono checkout (`CELEBI_DIR`). On Linux, use `yuki docker run` instead (auto-detects rootless Docker).
- **entrypoint.sh**: Starts in-container RabbitMQ (state in `/tmp`), waits for the broker, then `yuki server start`.
- **scripts/build.sh**: Local image builder — dev/prod targets, version or nightly tagging, optional tar export.

### CI/CD Pipeline
- **Pylint**: Runs on push with Python 3.8-3.10
- **Python Package**: Tests across Python 3.9-3.13 on master branch pushes/PRs
- **Docker Nightly**: Builds and pushes `ghcr.io` images every night at 02:17 UTC
- Package building and installation testing automated

## Common Development Tasks

### Adding New Routes
1. Create blueprint in `Yuki/server/routes/`
2. Register in `Yuki/server/app.py` `create_app()` function
3. Add tests in `UnitTest/test_server.py`

### Adding New Job Types
1. Extend `VJob` base class in `Yuki/kernel/jobs/base.py`
2. Implement required abstract methods
3. Update factory pattern in `VJob.create()` if needed

### Testing New Features
1. Follow existing mocking patterns in test classes
2. Use temporary directories for file system tests
3. Mock external dependencies (REANA, Celery)

## Important Files

- `pyproject.toml`: Build configuration and dependencies
- `docker-compose.yml`: Local development orchestration
- `docker/Dockerfile`: Multi-stage container image (dev/prod targets)
- `.github/workflows/docker-nightly.yml`: Nightly image CI
- `Yuki/main.py`: CLI entry point with Click commands
- `Yuki/server/app.py`: Flask application setup
- `Yuki/kernel/jobs/base.py`: Job abstraction and factory pattern
- `UnitTest/README.md`: Comprehensive test documentation
