"""Compatibility alias for :mod:`Yuki.kernel.runners.ssh_pool`."""
import sys

from .runners import ssh_pool as _implementation

sys.modules[__name__] = _implementation
