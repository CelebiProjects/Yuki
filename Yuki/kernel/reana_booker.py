"""Compatibility alias for :mod:`Yuki.server.services.reana_booking`."""
import sys

from Yuki.server.services import reana_booking as _implementation

sys.modules[__name__] = _implementation
