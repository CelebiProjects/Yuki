"""Compatibility alias for :mod:`Yuki.kernel.workflows.ihep`."""
import sys

from .workflows import ihep as _implementation

sys.modules[__name__] = _implementation
