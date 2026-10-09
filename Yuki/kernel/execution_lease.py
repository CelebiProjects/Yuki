"""Compatibility alias for :mod:`Yuki.kernel.execution.lease`."""
import sys

from .execution import lease as _implementation

sys.modules[__name__] = _implementation
