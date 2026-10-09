"""Compatibility alias for :mod:`Yuki.kernel.execution.submissions`."""
import sys

from .execution import submissions as _implementation

sys.modules[__name__] = _implementation
