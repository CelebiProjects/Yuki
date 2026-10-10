"""Shared names for runner-side impression cache publication."""

CACHE_COMPLETE_MARKER = ".yuki-cache-complete"
CACHE_IN_PROGRESS_MARKER = ".yuki-cache-in-progress"
WORKFLOW_FAILED_MARKER = "yuki.failed"


def is_cache_marker(path):
    """Return whether a cache-relative path is internal publication state."""
    return path in (CACHE_COMPLETE_MARKER, CACHE_IN_PROGRESS_MARKER)
