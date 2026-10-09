"""Compatibility alias for :mod:`Yuki.kernel.storage.liveness`."""
import sys

from .storage import liveness as _implementation

sys.modules[__name__] = _implementation
