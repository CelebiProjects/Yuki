"""Compatibility alias for :mod:`Yuki.kernel.storage.file_types`."""
import sys

from .storage import file_types as _implementation

sys.modules[__name__] = _implementation
