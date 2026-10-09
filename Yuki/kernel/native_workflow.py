"""Compatibility alias for :mod:`Yuki.kernel.workflows.native`."""
import sys

from .workflows import native as _implementation

sys.modules[__name__] = _implementation
