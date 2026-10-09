"""Compatibility alias for :mod:`Yuki.services.impression_transfer`."""
import sys

from Yuki.services import impression_transfer as _implementation

sys.modules[__name__] = _implementation
