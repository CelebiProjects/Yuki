"""Compatibility alias for :mod:`Yuki.utils.resource_units`."""
import sys

from Yuki.utils import resource_units as _implementation

sys.modules[__name__] = _implementation
