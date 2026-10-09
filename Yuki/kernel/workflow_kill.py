"""Compatibility alias for :mod:`Yuki.services.workflow_kill`."""
import sys

from Yuki.services import workflow_kill as _implementation

sys.modules[__name__] = _implementation
