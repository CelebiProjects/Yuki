"""Compatibility alias for :mod:`Yuki.kernel.jobs.base`."""
import sys

from .jobs import base as _implementation

sys.modules[__name__] = _implementation
