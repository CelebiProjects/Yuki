"""Compatibility alias for :mod:`Yuki.utils.locked_metadata`."""
import sys

from Yuki.utils import locked_metadata as _implementation

sys.modules[__name__] = _implementation
