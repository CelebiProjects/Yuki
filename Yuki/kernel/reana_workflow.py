"""Compatibility alias for :mod:`Yuki.kernel.workflows.reana`."""
import sys

from .workflows import reana as _implementation

sys.modules[__name__] = _implementation
