"""Runner environment mapping and normalization.

Supports both legacy plain-string values and new structured dict values
with ``type`` and ``value`` keys.
"""

from typing import Any, Dict, Optional

from CelebiChrono.utils import metadata


class EnvInterpreter:
    """Static utility class to read, write, and normalize conda_env_map entries."""

    @staticmethod
    def normalize_entry(entry: Any) -> dict:
        """Normalize a single conda_env_map entry.

        If *entry* is a plain string, wrap it in ``{"type": "conda", "value": entry}``.
        If *entry* is already a dict, return it unchanged.
        Otherwise raise ``TypeError``.

        Args:
            entry: The raw entry from conda_env_map.

        Returns:
            A dict with at least ``type`` and ``value`` keys.

        Raises:
            TypeError: If *entry* is neither a ``str`` nor a ``dict``.
        """
        if isinstance(entry, str):
            return {"type": "conda", "value": entry}
        if isinstance(entry, dict):
            if "type" not in entry or "value" not in entry:
                raise ValueError(
                    f"conda_env_map dict entry must contain 'type' and 'value' keys, got {entry!r}"
                )
            return entry
        raise TypeError(
            f"conda_env_map entry must be str or dict, got {type(entry).__name__}"
        )

    @staticmethod
    def resolve(environment: str, config_path: str) -> Optional[str]:
        """Resolve an environment name through the conda_env_map.

        Reads ``conda_env_map`` from *config_path* using :class:`metadata.ConfigFile`.
        If *environment* is present, the entry is normalised and its ``"value"``
        returned.  Returns ``None`` if the environment is not found or the config
        file / key is missing.

        Args:
            environment: The source environment identifier (e.g. Docker image).
            config_path: Absolute path to the JSON config file.

        Returns:
            The resolved environment value, or ``None``.
        """
        config = metadata.ConfigFile(config_path)
        env_map = config.read_variable("conda_env_map", {})
        if environment not in env_map:
            return None
        normalized = EnvInterpreter.normalize_entry(env_map[environment])
        return normalized.get("value")

    @staticmethod
    def add_mapping(config_path: str, source: str, env_type: str, value: str) -> None:
        """Add or update a mapping in ``conda_env_map``.

        Args:
            config_path: Absolute path to the JSON config file.
            source: The source environment identifier.
            env_type: The environment type (e.g. ``"conda"``, ``"venv"``).
            value: The environment value (e.g. env name or path).
        """
        config = metadata.ConfigFile(config_path)
        env_map = config.read_variable("conda_env_map", {})
        env_map[source] = {"type": env_type, "value": value}
        config.write_variable("conda_env_map", env_map)

    @staticmethod
    def remove_mapping(config_path: str, source: str) -> None:
        """Remove a mapping from ``conda_env_map`` if present.

        Args:
            config_path: Absolute path to the JSON config file.
            source: The source environment identifier to remove.
        """
        config = metadata.ConfigFile(config_path)
        env_map = config.read_variable("conda_env_map", {})
        if source in env_map:
            del env_map[source]
            config.write_variable("conda_env_map", env_map)

    @staticmethod
    def list_mappings(config_path: str) -> Dict[str, dict]:
        """Return all mappings from ``conda_env_map`` in normalised form.

        Args:
            config_path: Absolute path to the JSON config file.

        Returns:
            A dict mapping each source to its normalised ``{"type": ..., "value": ...}``
            entry.  Returns an empty dict if the config file is missing or the key
            is absent.
        """
        config = metadata.ConfigFile(config_path)
        env_map = config.read_variable("conda_env_map", {})
        return {k: EnvInterpreter.normalize_entry(v) for k, v in env_map.items()}
