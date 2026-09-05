"""Unit tests for ReanaBooker."""
# pylint: disable=protected-access
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from colored import Fore, Style
from Yuki.kernel.reana_booker import ReanaBooker


class TestReanaBooker(unittest.TestCase):
    """Test cases for ReanaBooker."""
    # pylint: disable=too-many-public-methods

    def setUp(self):
        """Set up test fixtures."""
        self.booker = ReanaBooker("https://reana.example.com", "test-token")

    def test_init(self):
        """Test initialization sets auth correctly."""
        print(Fore.BLUE + "Testing Init..." + Style.RESET)
        self.assertEqual(self.booker.server_url, "https://reana.example.com")
        self.assertEqual(self.booker.access_token, "test-token")
        self.assertTrue(self.booker.verify_ssl)

    def test_init_insecure(self):
        """Test initialization with verify_ssl=False."""
        print(Fore.BLUE + "Testing Init Insecure..." + Style.RESET)
        booker = ReanaBooker("https://reana.example.com", "test-token", verify_ssl=False)
        self.assertFalse(booker.verify_ssl)

    def test_setup_env(self):
        """Test _setup_env sets environment variables correctly."""
        print(Fore.BLUE + "Testing Setup Env..." + Style.RESET)
        self.booker._setup_env()
        self.assertEqual(os.environ["REANA_SERVER_URL"], "https://reana.example.com")
        self.assertNotIn("REANA_CLIENT_VERIFY_SSL", os.environ)

        booker_insecure = ReanaBooker("https://reana.example.com", "test-token", verify_ssl=False)
        booker_insecure._setup_env()
        self.assertEqual(os.environ["REANA_CLIENT_VERIFY_SSL"], "false")

    def test_should_ignore_exact_matches(self):
        """Test exact pattern matching for ignore rules."""
        print(Fore.BLUE + "Testing Should Ignore Exact..." + Style.RESET)
        self.assertTrue(self.booker._should_ignore(".celebi/config.local.json"))
        self.assertTrue(self.booker._should_ignore("test.pyc"))
        self.assertTrue(self.booker._should_ignore(".DS_Store"))
        self.assertTrue(self.booker._should_ignore("file.tmp"))
        self.assertTrue(self.booker._should_ignore("file.temp"))
        self.assertTrue(self.booker._should_ignore("backup~"))
        self.assertTrue(self.booker._should_ignore("file.swp"))
        self.assertTrue(self.booker._should_ignore("file.swo"))
        self.assertTrue(self.booker._should_ignore("file.~undo-tree~"))

    def test_should_ignore_directory_patterns(self):
        """Test directory prefix matching for ignore rules."""
        print(Fore.BLUE + "Testing Should Ignore Directory..." + Style.RESET)
        self.assertTrue(self.booker._should_ignore(".celebi/impressions/task1.tar.gz"))
        self.assertTrue(self.booker._should_ignore(".celebi/impressions_store/old.data"))
        self.assertTrue(self.booker._should_ignore(".git/config"))
        self.assertTrue(self.booker._should_ignore("__pycache__/module.cpython-39.pyc"))

    def test_should_not_ignore_kept_files(self):
        """Test that config.json and source files are not ignored."""
        print(Fore.BLUE + "Testing Should Not Ignore..." + Style.RESET)
        self.assertFalse(self.booker._should_ignore(".celebi/config.json"))
        self.assertFalse(self.booker._should_ignore("celebi.yaml"))
        self.assertFalse(self.booker._should_ignore("README.md"))
        self.assertFalse(self.booker._should_ignore("src/analysis.py"))
        self.assertFalse(self.booker._should_ignore("tasks/taskA/script.py"))

    def test_should_ignore_project_path_validation(self):
        """Test book_project validates project_path."""
        print(Fore.BLUE + "Testing Project Path Validation..." + Style.RESET)
        result = self.booker.book_project("/nonexistent/path", "test")
        self.assertFalse(result.success)
        self.assertIn("Invalid project path", str(result.messages))

    @patch("Yuki.kernel.reana_booker.reana_client.get_workflow_status")
    def test_get_workflow_found(self, mock_get_status):
        """Test finding an existing workflow."""
        print(Fore.BLUE + "Testing Get Workflow Found..." + Style.RESET)
        mock_get_status.return_value = {
            "name": "celebi-test-project",
            "id": "workflow-123",
            "status": "created",
        }

        result = self.booker._get_workflow("celebi-test-project")

        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "celebi-test-project")  # pylint: disable=unsubscriptable-object
        self.assertEqual(result["id"], "workflow-123")  # pylint: disable=unsubscriptable-object
        mock_get_status.assert_called_once()
        call_kwargs = mock_get_status.call_args.kwargs
        self.assertEqual(call_kwargs["workflow"], "celebi-test-project")
        self.assertEqual(call_kwargs["access_token"], "test-token")

    @patch("Yuki.kernel.reana_booker.reana_client.get_workflow_status")
    def test_get_workflow_not_found(self, mock_get_status):
        """Test when workflow does not exist."""
        print(Fore.BLUE + "Testing Get Workflow Not Found..." + Style.RESET)
        mock_get_status.side_effect = Exception("Workflow does not exist")

        result = self.booker._get_workflow("celebi-nonexistent")

        self.assertIsNone(result)

    @patch("Yuki.kernel.reana_booker.reana_client.get_workflow_status")
    def test_get_workflow_connection_error(self, mock_get_status):
        """Test handling connection error during workflow lookup."""
        print(Fore.BLUE + "Testing Get Workflow Connection Error..." + Style.RESET)
        mock_get_status.side_effect = Exception("Connection refused")

        result = self.booker._get_workflow("celebi-test")

        self.assertIsNone(result)

    @patch("Yuki.kernel.reana_booker.reana_client.create_workflow")
    @patch("builtins.open", unittest.mock.mock_open(read_data="version: 0.8.0"))
    def test_create_workflow(self, mock_create_workflow):
        """Test creating a new workflow."""
        print(Fore.BLUE + "Testing Create Workflow..." + Style.RESET)
        mock_create_workflow.return_value = {
            "workflow_id": "workflow-456",
            "workflow_name": "celebi-test"
        }

        result = self.booker._create_workflow("celebi-test")

        self.assertEqual(result["workflow_id"], "workflow-456")
        mock_create_workflow.assert_called_once()
        call_kwargs = mock_create_workflow.call_args.kwargs
        self.assertEqual(call_kwargs["name"], "celebi-test")
        self.assertEqual(call_kwargs["access_token"], "test-token")
        self.assertIn("reana_specification", call_kwargs)

    @patch("Yuki.kernel.reana_booker.reana_client.create_workflow")
    def test_create_workflow_with_repo_metadata(self, mock_create_workflow):
        """Test creating a workflow with reana_repo metadata."""
        print(Fore.BLUE + "Testing Create Workflow with Repo Metadata..." + Style.RESET)
        mock_create_workflow.return_value = {
            "workflow_id": "workflow-789",
            "workflow_name": "celebi-test"
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a fake task directory with config
            task_dir = os.path.join(tmpdir, "tasks", "taskA")
            os.makedirs(task_dir)
            os.makedirs(os.path.join(task_dir, ".celebi"))
            with open(os.path.join(task_dir, ".celebi", "config.json"), "w", encoding="utf-8") as f:
                json.dump({"object_type": "task", "impression": "imp-abc-123"}, f)
            with open(os.path.join(task_dir, "celebi.yaml"), "w", encoding="utf-8") as f:
                f.write("descriptor: taskA\nenvironment: alpine\n")

            result = self.booker._create_workflow("celebi-test", tmpdir)

        self.assertEqual(result["workflow_id"], "workflow-789")
        mock_create_workflow.assert_called_once()
        call_kwargs = mock_create_workflow.call_args.kwargs
        spec = call_kwargs["reana_specification"]
        self.assertIn("reana_repo", spec)
        self.assertEqual(spec["reana_repo"]["project_name"], os.path.basename(tmpdir))
        self.assertEqual(len(spec["reana_repo"]["objects"]), 1)
        self.assertEqual(spec["reana_repo"]["objects"][0]["type"], "task")
        self.assertEqual(spec["reana_repo"]["objects"][0]["descriptor"], "taskA")
        self.assertEqual(spec["reana_repo"]["objects"][0]["impression"], "imp-abc-123")

    @patch("Yuki.kernel.reana_booker.reana_client.create_workflow")
    def test_create_workflow_with_local_config_override(self, mock_create_workflow):
        """Test that config.local.json overrides config.json for impression."""
        print(Fore.BLUE + "Testing Local Config Override..." + Style.RESET)
        mock_create_workflow.return_value = {
            "workflow_id": "workflow-999",
            "workflow_name": "celebi-test"
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            task_dir = os.path.join(tmpdir, "taskA")
            os.makedirs(task_dir)
            os.makedirs(os.path.join(task_dir, ".celebi"))
            # config.json has one impression
            with open(os.path.join(task_dir, ".celebi", "config.json"), "w", encoding="utf-8") as f:
                json.dump({"object_type": "task", "impression": "imp-from-config"}, f)
            # config.local.json overrides it
            with open(os.path.join(task_dir, ".celebi", "config.local.json"),
                      "w", encoding="utf-8") as f:
                json.dump({"impression": "imp-from-local"}, f)

            result = self.booker._create_workflow("celebi-test", tmpdir)

        self.assertEqual(result["workflow_id"], "workflow-999")
        call_kwargs = mock_create_workflow.call_args.kwargs
        spec = call_kwargs["reana_specification"]
        # config.local.json should override the impression
        self.assertEqual(spec["reana_repo"]["objects"][0]["impression"], "imp-from-local")

    def test_sanitize_upload_path(self):
        """Test hidden directory name sanitization."""
        print(Fore.BLUE + "Testing Sanitize Upload Path..." + Style.RESET)
        # Hidden directory -> dot_ prefix
        self.assertEqual(
            self.booker._sanitize_upload_path(".celebi/config.json"),
            "dot_celebi/config.json"
        )
        self.assertEqual(
            self.booker._sanitize_upload_path("tasks/.hidden/file.txt"),
            "tasks/dot_hidden/file.txt"
        )
        # Normal paths unchanged
        self.assertEqual(
            self.booker._sanitize_upload_path("src/main.py"),
            "src/main.py"
        )
        self.assertEqual(
            self.booker._sanitize_upload_path("README.md"),
            "README.md"
        )
        # Nested hidden dirs
        self.assertEqual(
            self.booker._sanitize_upload_path(".a/.b/file.txt"),
            "dot_a/dot_b/file.txt"
        )

    @patch("Yuki.kernel.reana_booker.reana_client.upload_file")
    def test_upload_files(self, mock_upload_file):
        """Test uploading files to workflow workspace."""
        print(Fore.BLUE + "Testing Upload Files..." + Style.RESET)
        mock_upload_file.return_value = {"message": "File uploaded"}

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create files
            with open(os.path.join(tmpdir, "README.md"), "w", encoding="utf-8") as f:
                f.write("# Test")
            os.makedirs(os.path.join(tmpdir, "src"))
            with open(os.path.join(tmpdir, "src", "analysis.py"), "w", encoding="utf-8") as f:
                f.write("print('hello')")
            # Create file in hidden dir (should be sanitized)
            os.makedirs(os.path.join(tmpdir, ".celebi"))
            with open(os.path.join(tmpdir, ".celebi", "config.json"), "w", encoding="utf-8") as f:
                f.write('{"test": true}')
            # Create ignored file
            os.makedirs(os.path.join(tmpdir, ".celebi", "impressions"))
            with open(os.path.join(tmpdir, ".celebi", "impressions", "cache.tar"),
                      "w", encoding="utf-8") as f:
                f.write("ignored")

            self.booker._upload_files("workflow-123", tmpdir)

        calls = mock_upload_file.call_args_list
        uploaded_files = []
        for call in calls:
            kwargs = call.kwargs
            uploaded_files.append(kwargs.get("file_name", ""))

        self.assertIn("README.md", uploaded_files)
        self.assertIn("src/analysis.py", uploaded_files)
        # .celebi/config.json should be sanitized to dot_celebi/config.json
        self.assertIn("dot_celebi/config.json", uploaded_files)
        # cache.tar should NOT be uploaded (ignored)
        self.assertTrue(
            all("cache.tar" not in f for f in uploaded_files),
            "cache.tar should not be uploaded"
        )
        # Verify access_token and workflow are in kwargs for each upload
        for call in calls:
            kwargs = call.kwargs
            self.assertEqual(kwargs.get("access_token"), "test-token")
            self.assertEqual(kwargs.get("workflow"), "workflow-123")

    @patch("Yuki.kernel.reana_booker.reana_client.upload_file")
    def test_upload_stageout_files(self, mock_upload_file):
        """Test uploading stageout files from Yuki storage."""
        print(Fore.BLUE + "Testing Upload Stageout Files..." + Style.RESET)
        mock_upload_file.return_value = {"message": "File uploaded"}

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create project structure
            os.makedirs(os.path.join(tmpdir, ".celebi"))
            with open(os.path.join(tmpdir, ".celebi", "config.json"), "w", encoding="utf-8") as f:
                json.dump({"object_type": "project", "project_uuid": "proj-123"}, f)

            # Create Yuki storage with stageout files
            # Structure: Storage/{project}/{impression}/{runner}/stageout/
            yuki_home = os.path.join(tmpdir, ".Yuki")
            stageout_dir = os.path.join(yuki_home, "Storage", "proj-123",
                                        "imp-abc", "runner-1", "stageout")
            os.makedirs(stageout_dir)
            with open(os.path.join(stageout_dir, "output.root"), "w", encoding="utf-8") as f:
                f.write("root data")
            with open(os.path.join(stageout_dir, "plots.pdf"), "w", encoding="utf-8") as f:
                f.write("pdf data")

            repo_metadata = {
                "objects": [
                    {"path": "taskA", "type": "task", "impression": "imp-abc"},
                ]
            }

            with patch.dict(os.environ, {"YUKIDIR": yuki_home}):
                self.booker._upload_stageout_files(
                    "workflow-123", tmpdir, repo_metadata, upload_mode="all"
                )

        calls = mock_upload_file.call_args_list
        uploaded_files = [c.kwargs.get("file_name", "") for c in calls]

        self.assertIn("impression_data/imp-abc/stageout/output.root",
                      uploaded_files)
        self.assertIn("impression_data/imp-abc/stageout/plots.pdf",
                      uploaded_files)
        # Verify workflow and access_token in each call
        for call in calls:
            kwargs = call.kwargs
            self.assertEqual(kwargs.get("access_token"), "test-token")
            self.assertEqual(kwargs.get("workflow"), "workflow-123")

    @patch("Yuki.kernel.reana_booker.reana_client.upload_file")
    def test_upload_files_skips_unchanged_objects(self, mock_upload_file):
        """Test skipping upload for objects with matching impression UUID."""
        print(Fore.BLUE + "Testing Upload Files Skip Unchanged..." + Style.RESET)
        mock_upload_file.return_value = {"message": "File uploaded"}

        with tempfile.TemporaryDirectory() as tmpdir:
            # taskA - unchanged (should be skipped)
            task_a_dir = os.path.join(tmpdir, "taskA")
            os.makedirs(task_a_dir)
            with open(os.path.join(task_a_dir, "script.py"), "w", encoding="utf-8") as f:
                f.write("print('taskA')")
            # taskB - changed (should be uploaded)
            task_b_dir = os.path.join(tmpdir, "taskB")
            os.makedirs(task_b_dir)
            with open(os.path.join(task_b_dir, "script.py"), "w", encoding="utf-8") as f:
                f.write("print('taskB')")
            # Top-level file (always uploaded)
            with open(os.path.join(tmpdir, "README.md"), "w", encoding="utf-8") as f:
                f.write("# Test")

            skip_prefixes = {"taskA"}
            object_paths = ["taskA", "taskB"]

            self.booker._upload_files(
                "workflow-123", tmpdir,
                skip_prefixes=skip_prefixes,
                object_paths=object_paths
            )

        calls = mock_upload_file.call_args_list
        uploaded_files = [c.kwargs.get("file_name", "") for c in calls]

        # taskA files should be skipped
        self.assertNotIn("taskA/script.py", uploaded_files)
        # taskB and top-level files should be uploaded
        self.assertIn("taskB/script.py", uploaded_files)
        self.assertIn("README.md", uploaded_files)

    def test_get_object_prefix_for_file(self):
        """Test mapping files to their parent object paths."""
        print(Fore.BLUE + "Testing Get Object Prefix..." + Style.RESET)
        object_paths = ["GenTask", "GenTask/sub", "FitTask"]

        self.assertEqual(
            self.booker._get_object_prefix_for_file("GenTask/celebi.yaml", object_paths),
            "GenTask"
        )
        self.assertEqual(
            self.booker._get_object_prefix_for_file("GenTask/sub/file.txt", object_paths),
            "GenTask/sub"
        )
        self.assertEqual(
            self.booker._get_object_prefix_for_file("FitTask/script.py", object_paths),
            "FitTask"
        )
        self.assertEqual(
            self.booker._get_object_prefix_for_file("README.md", object_paths),
            ""
        )

    @patch("Yuki.kernel.reana_booker.reana_client.upload_file")
    @patch("Yuki.kernel.reana_booker.reana_client.create_workflow")
    @patch("Yuki.kernel.reana_booker.reana_client.get_workflow_status")
    def test_book_project_existing_workflow(
        self, mock_get_status, mock_create_workflow, mock_upload_file
    ):
        """Test booking with existing workflow."""
        print(Fore.BLUE + "Testing Book Project Existing..." + Style.RESET)
        # Mock existing workflow
        mock_get_status.return_value = {"name": "celebi-test", "id": "wf-123", "status": "created"}
        mock_upload_file.return_value = {"message": "File uploaded"}

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "README.md"), "w", encoding="utf-8") as f:
                f.write("# Test")

            result = self.booker.book_project(tmpdir, "test")

        self.assertTrue(result.success)
        self.assertIn("Using existing REANA workflow", str(result.messages))
        mock_create_workflow.assert_not_called()
        mock_upload_file.assert_called()

    @patch("Yuki.kernel.reana_booker.reana_client.upload_file")
    @patch("Yuki.kernel.reana_booker.reana_client.create_workflow")
    @patch("Yuki.kernel.reana_booker.reana_client.get_workflow_status")
    def test_book_project_new_workflow(
        self, mock_get_status, mock_create_workflow, mock_upload_file
    ):
        """Test booking creating new workflow."""
        print(Fore.BLUE + "Testing Book Project New..." + Style.RESET)
        # Mock no existing workflow
        mock_get_status.side_effect = Exception("Workflow does not exist")
        # Mock create workflow
        mock_create_workflow.return_value = {
            "workflow_id": "wf-456", "workflow_name": "celebi-test"
        }
        mock_upload_file.return_value = {"message": "File uploaded"}

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "README.md"), "w", encoding="utf-8") as f:
                f.write("# Test")

            result = self.booker.book_project(tmpdir, "test")

        self.assertTrue(result.success)
        self.assertIn("Creating REANA workflow", str(result.messages))
        mock_create_workflow.assert_called_once()
        mock_upload_file.assert_called()


if __name__ == "__main__":
    unittest.main()
