"""Conversions for workflow resource quantities."""
import re
from decimal import Decimal, InvalidOperation, ROUND_CEILING


_MEMORY_PATTERN = re.compile(
    r"^([0-9]+(?:\.[0-9]+)?)([KMGT](?:I?B?)?)?$", re.IGNORECASE)
_MEMORY_TO_MB = {
    "": Decimal(1),
    "K": Decimal(1) / Decimal(1024),
    "KB": Decimal(1) / Decimal(1024),
    "KI": Decimal(1) / Decimal(1024),
    "KIB": Decimal(1) / Decimal(1024),
    "M": Decimal(1),
    "MB": Decimal(1),
    "MI": Decimal(1),
    "MIB": Decimal(1),
    "G": Decimal(1024),
    "GB": Decimal(1024),
    "GI": Decimal(1024),
    "GIB": Decimal(1024),
    "T": Decimal(1024 * 1024),
    "TB": Decimal(1024 * 1024),
    "TI": Decimal(1024 * 1024),
    "TIB": Decimal(1024 * 1024),
}


def memory_to_mb(value):
    """Convert a Celebi memory quantity to a positive integer MB value.

    Returns ``None`` for a missing, non-positive, or unsupported quantity so
    runner-specific code can apply its configured fallback.
    """
    normalized = re.sub(r"\s+", "", str(value or ""))
    match = _MEMORY_PATTERN.fullmatch(normalized)
    if not match:
        return None
    try:
        amount = Decimal(match.group(1))
    except InvalidOperation:
        return None
    if amount <= 0:
        return None
    multiplier = _MEMORY_TO_MB.get((match.group(2) or "").upper())
    if multiplier is None:
        return None
    return max(1, int((amount * multiplier).to_integral_value(
        rounding=ROUND_CEILING)))
