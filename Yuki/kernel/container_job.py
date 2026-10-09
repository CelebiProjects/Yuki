"""Compatibility alias for :mod:`Yuki.kernel.jobs.container`."""
import sys

from .jobs import container as _implementation

sys.modules[__name__] = _implementation
