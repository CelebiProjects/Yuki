"""Compatibility alias for :mod:`Yuki.kernel.workflows.base`."""
import sys

from .workflows import base as _implementation

sys.modules[__name__] = _implementation
