"""Compatibility alias for :mod:`Yuki.services.workflow_purge`."""
import sys

from Yuki.services import workflow_purge as _implementation

sys.modules[__name__] = _implementation
