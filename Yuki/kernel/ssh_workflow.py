"""Compatibility alias for :mod:`Yuki.kernel.workflows.ssh`."""
import sys

from .workflows import ssh as _implementation

sys.modules[__name__] = _implementation
