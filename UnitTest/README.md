# Yuki Server Unit Tests

This directory contains comprehensive unit tests for the Yuki server module.

## Test Coverage

The test suite covers:

### Flask Routes
- `/upload` - File upload functionality (GET/POST)
- `/execute` - Job execution endpoint
- `/setjobstatus/<impression>/<status>` - Job status setting
- `/download/<filename>` - File download
- `/export/<impression>/<filename>` - File export
- `/impview/<impression>` - Impression view with file listing
- `/fileview/<impression>/<runner_id>/<filename>` - Individual file view
- `/kill/<impression>` - Kill running jobs
- `/runners` - List available runners
- `/runnersurl` - Get runner URLs
- `/runnerconnection/<runner>` - Test runner connections
- `/registerrunner` - Register new runners
- `/removerunner/<runner>` - Remove runners
- `/status/<impression>` - Get job status
- `/deposited/<impression>` - Check if impression exists
- `/ditestatus` - Health check
- `/run/<impression>/<machine>` - Run specific impression
- `/machine_id/<machine>` - Get machine ID
- `/outputs/<impression>/<machine>` - Get job outputs
- `/getfile/<impression>/<filename>` - Get specific files
- `/impression/<impression>` - Get impression path
- `/collect/<impression>` - Collect finished jobs
- `/workflow/<impression>` - Get workflow information
- `/samplestatus/<impression>` - Get sample status
- `/` - Index page

### Celery Tasks
- `task_exec_impression` - Asynchronous job execution
- `task_update_workflow_status` - Workflow status updates

### Utility Functions
- `ping` - REANA server connectivity testing
- `server_start` - Server startup process

## Running Tests

### Option 1: Using the test runner script
```bash
python run_tests.py
```

### Option 2: Using unittest directly
```bash
python -m unittest UnitTest.test_server -v
```

### Option 3: Using pytest (if installed)
```bash
pytest UnitTest/test_server.py -v
```

### Running specific tests
```bash
# Run a specific test class
python -m unittest UnitTest.test_server.TestYukiServer -v

# Run a specific test method
python -m unittest UnitTest.test_server.TestYukiServer.test_upload_file_post -v

# Using the test runner script for specific tests
python run_tests.py TestYukiServer.test_upload_file_post
```

## Test Structure

The tests are organized into three main test classes:

1. **TestYukiServer** - Tests for Flask routes and endpoints
2. **TestCeleryTasks** - Tests for Celery background tasks
3. **TestUtilityFunctions** - Tests for utility and helper functions

## Mocking Strategy

The tests use extensive mocking to:
- Isolate units under test
- Avoid external dependencies (databases, file systems, network calls)
- Control test environments and inputs
- Ensure tests run quickly and reliably

Key mocking includes:
- File system operations (`os.path.exists`, `os.path.join`, `os.listdir`)
- Flask file uploads and responses
- Celery task execution
- VJob, VWorkflow, VContainer classes
- Configuration file access
- External API calls (REANA)

## Dependencies

The tests require:
- `unittest` (built-in)
- `unittest.mock` (built-in)
- Flask test client capabilities
- Temporary file and directory creation

Optional dependencies:
- `pytest` for alternative test running
- `pytest-mock` for enhanced mocking capabilities

## Notes

- Tests automatically skip if Yuki modules cannot be imported
- Temporary directories are created and cleaned up for each test
- All external dependencies are mocked to ensure test isolation
- Tests include both positive and negative test cases
- Error conditions and edge cases are covered
