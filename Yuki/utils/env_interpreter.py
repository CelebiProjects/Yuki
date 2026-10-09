"""Compatibility alias for :mod:`Yuki.kernel.runners.environments`."""
import sys

from Yuki.kernel.runners import environments as _implementation

sys.modules[__name__] = _implementation
