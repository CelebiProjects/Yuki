"""Unit tests for Yuki.kernel.runners.environments.EnvInterpreter."""
import os
import shutil
import tempfile
import unittest

from CelebiChrono.utils import metadata

# Import the module under test
from Yuki.kernel.runners.environments import EnvInterpreter


class TestEnvInterpreter(unittest.TestCase):
    """Test cases for EnvInterpreter static methods."""
    # pylint: disable=too-many-public-methods

    def setUp(self):
        """Create a temporary directory for config files."""
        self.tmpdir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.tmpdir, "config.json")

    def tearDown(self):
        """Remove the temporary directory."""
        shutil.rmtree(self.tmpdir)

    # ------------------------------------------------------------------
    # normalize_entry
    # ------------------------------------------------------------------

    def test_normalize_entry_string(self):
        """Plain string should become {"type": "conda", "value": entry}."""
        result = EnvInterpreter.normalize_entry("myenv")
        self.assertEqual(result, {"type": "conda", "value": "myenv"})

    def test_normalize_entry_dict(self):
        """Dict should be returned as-is."""
        entry = {"type": "venv", "value": "/path/to/venv"}
        result = EnvInterpreter.normalize_entry(entry)
        self.assertEqual(result, entry)

    def test_normalize_entry_dict_missing_keys(self):
        """Dict missing 'type' or 'value' should raise ValueError."""
        with self.assertRaises(ValueError):
            EnvInterpreter.normalize_entry({"type": "conda"})
        with self.assertRaises(ValueError):
            EnvInterpreter.normalize_entry({"value": "env"})

    def test_normalize_entry_empty_dict(self):
        """Empty dict should raise ValueError."""
        with self.assertRaises(ValueError):
            EnvInterpreter.normalize_entry({})

    def test_normalize_entry_invalid_type(self):
        """Non-string, non-dict should raise TypeError."""
        with self.assertRaises(TypeError):
            EnvInterpreter.normalize_entry(123)
        with self.assertRaises(TypeError):
            EnvInterpreter.normalize_entry(None)
        with self.assertRaises(TypeError):
            EnvInterpreter.normalize_entry(["a", "b"])

    # ------------------------------------------------------------------
    # resolve
    # ------------------------------------------------------------------

    def test_resolve_mapped_dict(self):
        """Resolve should return the value for a dict entry."""
        config = metadata.ConfigFile(self.config_path)
        config.write_variable("conda_env_map", {
            "image1": {"type": "conda", "value": "env1"}
        })
        result = EnvInterpreter.resolve("image1", self.config_path)
        self.assertEqual(result, "env1")

    def test_resolve_mapped_plain_string(self):
        """Resolve should return the value for a plain string entry."""
        config = metadata.ConfigFile(self.config_path)
        config.write_variable("conda_env_map", {
            "image2": "env2"
        })
        result = EnvInterpreter.resolve("image2", self.config_path)
        self.assertEqual(result, "env2")

    def test_resolve_not_found(self):
        """Resolve should return None when environment is not in map."""
        config = metadata.ConfigFile(self.config_path)
        config.write_variable("conda_env_map", {
            "image3": "env3"
        })
        result = EnvInterpreter.resolve("missing", self.config_path)
        self.assertIsNone(result)

    def test_resolve_missing_config(self):
        """Resolve should return None when config file does not exist."""
        missing_path = os.path.join(self.tmpdir, "nonexistent.json")
        result = EnvInterpreter.resolve("image", missing_path)
        self.assertIsNone(result)

    def test_resolve_missing_conda_env_map_key(self):
        """Resolve should return None when conda_env_map key is absent."""
        config = metadata.ConfigFile(self.config_path)
        config.write_variable("other_key", "value")
        result = EnvInterpreter.resolve("image", self.config_path)
        self.assertIsNone(result)

    def test_resolve_malformed_dict_raises(self):
        """Resolve should raise ValueError when config contains a malformed dict entry."""
        config = metadata.ConfigFile(self.config_path)
        config.write_variable("conda_env_map", {
            "docker:bad": {"type": "conda"}  # missing value
        })
        with self.assertRaises(ValueError):
            EnvInterpreter.resolve("docker:bad", self.config_path)

    # ------------------------------------------------------------------
    # add_mapping
    # ------------------------------------------------------------------

    def test_add_mapping_new_entry(self):
        """add_mapping should create a new entry."""
        EnvInterpreter.add_mapping(self.config_path, "src1", "conda", "env1")
        config = metadata.ConfigFile(self.config_path)
        mapping = config.read_variable("conda_env_map", {})
        self.assertEqual(mapping, {"src1": {"type": "conda", "value": "env1"}})

    def test_add_mapping_overwrite_existing(self):
        """add_mapping should overwrite an existing entry."""
        config = metadata.ConfigFile(self.config_path)
        config.write_variable("conda_env_map", {
            "src2": {"type": "conda", "value": "old_env"}
        })
        EnvInterpreter.add_mapping(self.config_path, "src2", "venv", "/new/path")
        mapping = config.read_variable("conda_env_map", {})
        self.assertEqual(mapping, {"src2": {"type": "venv", "value": "/new/path"}})

    def test_add_mapping_creates_conda_env_map(self):
        """add_mapping should create conda_env_map if it does not exist."""
        EnvInterpreter.add_mapping(self.config_path, "src3", "conda", "env3")
        config = metadata.ConfigFile(self.config_path)
        mapping = config.read_variable("conda_env_map", {})
        self.assertEqual(mapping, {"src3": {"type": "conda", "value": "env3"}})

    # ------------------------------------------------------------------
    # remove_mapping
    # ------------------------------------------------------------------

    def test_remove_mapping_existing(self):
        """remove_mapping should delete an existing entry."""
        config = metadata.ConfigFile(self.config_path)
        config.write_variable("conda_env_map", {
            "src4": {"type": "conda", "value": "env4"},
            "src5": {"type": "conda", "value": "env5"}
        })
        EnvInterpreter.remove_mapping(self.config_path, "src4")
        mapping = config.read_variable("conda_env_map", {})
        self.assertEqual(mapping, {"src5": {"type": "conda", "value": "env5"}})

    def test_remove_mapping_not_found(self):
        """remove_mapping should not error when source is not present."""
        config = metadata.ConfigFile(self.config_path)
        config.write_variable("conda_env_map", {
            "src6": "env6"
        })
        EnvInterpreter.remove_mapping(self.config_path, "missing")
        mapping = config.read_variable("conda_env_map", {})
        self.assertEqual(mapping, {"src6": "env6"})

    # ------------------------------------------------------------------
    # list_mappings
    # ------------------------------------------------------------------

    def test_list_mappings_with_entries(self):
        """list_mappings should return all entries normalized."""
        config = metadata.ConfigFile(self.config_path)
        config.write_variable("conda_env_map", {
            "img1": "env1",
            "img2": {"type": "venv", "value": "/path"}
        })
        result = EnvInterpreter.list_mappings(self.config_path)
        self.assertEqual(result, {
            "img1": {"type": "conda", "value": "env1"},
            "img2": {"type": "venv", "value": "/path"}
        })

    def test_list_mappings_empty(self):
        """list_mappings should return {} when conda_env_map is empty."""
        config = metadata.ConfigFile(self.config_path)
        config.write_variable("conda_env_map", {})
        result = EnvInterpreter.list_mappings(self.config_path)
        self.assertEqual(result, {})

    def test_list_mappings_missing_file(self):
        """list_mappings should return {} when config file does not exist."""
        missing_path = os.path.join(self.tmpdir, "nonexistent.json")
        result = EnvInterpreter.list_mappings(missing_path)
        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
