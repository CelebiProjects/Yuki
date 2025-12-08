"""
Configuration management for Yuki server.
"""
import os
from CelebiChrono.utils.metadata import ConfigFile


class YukiConfig:
    """Centralized configuration management for Yuki server."""

    def __init__(self):
        self.home_dir = os.environ["HOME"]
        self.config_path = os.path.join(self.home_dir, ".Yuki", "config.json")
        self.storage_path = os.path.join(self.home_dir, ".Yuki", "Storage")
        self.daemon_path = os.path.join(self.home_dir, ".Yuki", "daemon")

    def get_config_file(self):
        """Get ConfigFile instance for runner configuration."""
        return ConfigFile(self.config_path)

    def get_job_path(self, impression):
        """Get path for a specific job/impression."""
        return os.path.join(self.storage_path, impression)

    def get_job_config_path(self, impression):
        """Get config file path for a specific job/impression."""
        return os.path.join(self.get_job_path(impression), "config.json")


# Global config instance
config = YukiConfig()
