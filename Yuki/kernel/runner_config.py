"""Compatibility alias for :mod:`Yuki.kernel.runners.config`."""
import sys

from .runners import config as _implementation

sys.modules[__name__] = _implementation
