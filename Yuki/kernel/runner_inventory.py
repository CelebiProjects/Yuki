"""Compatibility alias for :mod:`Yuki.services.runner_inventory`."""
import sys

from Yuki.services import runner_inventory as _implementation

sys.modules[__name__] = _implementation
