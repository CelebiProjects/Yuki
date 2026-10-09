"""Compatibility alias for :mod:`Yuki.kernel.execution.progress`."""
import sys

from .execution import progress as _implementation

sys.modules[__name__] = _implementation
