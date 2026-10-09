"""Compatibility alias for :mod:`Yuki.kernel.workflows.snakefile`."""
import sys

from Yuki.kernel.workflows import snakefile as _implementation

sys.modules[__name__] = _implementation
