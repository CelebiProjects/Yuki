"""Compatibility alias for :mod:`Yuki.kernel.jobs.image`."""
import sys

from .jobs import image as _implementation

sys.modules[__name__] = _implementation
