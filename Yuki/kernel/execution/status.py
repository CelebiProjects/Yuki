"""
Status constants and translation utilities for Yuki job status system.

This module defines musical-themed status names and provides translation
between legacy status names and new musical names.
"""

# Musical status names (new system)
SILENCE = "silence"              # Initial state, nothing happening
PRELUDE = "prelude"              # Preparation phase before execution
IN_MOVEMENT = "in movement"      # Active execution phase
COMPOSING = "composing"          # Constructing/assembling components
ORCHESTRATING = "orchestrating"  # Final preparation before execution
TUNING = "tuning"                # Preparing algorithm components
DISSONANCE = "dissonance"        # Workflow construction failed - harmony broken before execution
CODA = "coda"                    # Successful conclusion
FINAL_NOTE = "final note"        # Permanent storage

# Legacy status names that remain unchanged
FAILED = "failed"            # Backend execution failed
STOPPED = "stopped"          # User-interrupted execution
DELETED = "deleted"          # Removed from performance
ARCHIVED = "archived"        # Archived jobs (kept for backward compatibility)
PENDING = "pending"

# Statuses reached before execution starts: the runner cannot hold any
# files yet, so a live listing would always be empty. Includes legacy
# REANA statuses ("created", "queued") that have no musical translation.
PRE_EXECUTION_STATUSES = (
    SILENCE, PRELUDE, COMPOSING, ORCHESTRATING, TUNING, DISSONANCE, PENDING,
    "created", "queued",
)

# Legacy status names (old system)
LEGACY_RAW = "raw"
LEGACY_WAITING = "waiting"
LEGACY_RUNNING = "running"
LEGACY_SUBMITTED = "submitted"
LEGACY_BUILT = "built"
LEGACY_READY = "ready"
LEGACY_FINISHED = "finished"
LEGACY_SUCCESS = "success"
LEGACY_CREATED = "created"
LEGACY_KILLED = "killed"

# Mapping from legacy names to musical names
LEGACY_TO_MUSICAL = {
    LEGACY_RAW: SILENCE,
    LEGACY_WAITING: PRELUDE,
    LEGACY_RUNNING: IN_MOVEMENT,
    LEGACY_SUBMITTED: COMPOSING,
    LEGACY_BUILT: ORCHESTRATING,
    LEGACY_READY: TUNING,
    LEGACY_FINISHED: CODA,
    LEGACY_SUCCESS: CODA,
    LEGACY_KILLED: STOPPED,
    # Note: "failed" maps differently based on context
    # "stopped", "deleted", "archived" remain unchanged
}

# Mapping from musical names to legacy names (for backward compatibility)
MUSICAL_TO_LEGACY = {
    SILENCE: LEGACY_RAW,
    PRELUDE: LEGACY_WAITING,
    IN_MOVEMENT: LEGACY_RUNNING,
    COMPOSING: LEGACY_SUBMITTED,
    ORCHESTRATING: LEGACY_BUILT,
    TUNING: LEGACY_READY,
    DISSONANCE: FAILED,  # Maps to "failed" for backward compatibility
    CODA: LEGACY_SUCCESS,
    FINAL_NOTE: ARCHIVED,
    # These remain the same in both systems
    FAILED: FAILED,
    STOPPED: STOPPED,
    DELETED: DELETED,
    ARCHIVED: ARCHIVED,
}

# All valid status names in the new system
VALID_STATUSES = {
    SILENCE, PRELUDE, IN_MOVEMENT, COMPOSING, ORCHESTRATING,
    TUNING, DISSONANCE, CODA, FINAL_NOTE,
    FAILED, STOPPED, DELETED, ARCHIVED, PENDING
}

# All valid status names in the legacy system
VALID_LEGACY_STATUSES = {
    LEGACY_RAW, LEGACY_WAITING, LEGACY_RUNNING, LEGACY_SUBMITTED,
    LEGACY_BUILT, LEGACY_READY, LEGACY_FINISHED, LEGACY_SUCCESS,
    LEGACY_KILLED, FAILED, STOPPED, DELETED, ARCHIVED
}


def translate_to_musical(status: str) -> str:
    """
    Translate a legacy status name to its musical equivalent.

    Args:
        status: Legacy status name

    Returns:
        Musical status name, or the original status if no translation exists
    """
    # If it's already a musical status, return it
    if status in VALID_STATUSES:
        return status

    # Translate from legacy to musical
    return LEGACY_TO_MUSICAL.get(status, status)


def translate_to_legacy(status: str) -> str:
    """
    Translate a musical status name to its legacy equivalent.

    Args:
        status: Musical status name

    Returns:
        Legacy status name, or the original status if no translation exists
    """
    # If it's already a legacy status, return it
    if status in VALID_LEGACY_STATUSES:
        return status

    # Translate from musical to legacy
    return MUSICAL_TO_LEGACY.get(status, status)


def is_valid_status(status: str) -> bool:
    """
    Check if a status is valid in either the new or legacy system.

    Args:
        status: Status name to check

    Returns:
        True if the status is valid, False otherwise
    """
    return status in VALID_STATUSES or status in VALID_LEGACY_STATUSES


def get_detailed_status_message(status: str, context: dict = None) -> str:
    """
    Generate a default detailed status message for a given status.

    Args:
        status: Status name (musical or legacy)
        context: Optional context dictionary with additional information

    Returns:
        Default detailed status message
    """
    musical_status = translate_to_musical(status)
    context = context or {}

    messages = {
        SILENCE: "Initial state - no operations have been performed",
        PRELUDE: "Waiting for execution resources to become available",
        IN_MOVEMENT: "Executing workflow steps",
        COMPOSING: "Building and assembling job components",
        ORCHESTRATING: "Final preparation before execution",
        TUNING: "Configuring algorithm parameters and dependencies",
        DISSONANCE: "Workflow construction failed - unable to proceed to execution",
        CODA: "Successfully completed all workflow steps",
        FINAL_NOTE: "Job has been permanently archived",
        FAILED: "Backend execution failed during runtime",
        STOPPED: "Job execution was manually stopped by user",
        DELETED: "Job has been removed from the system",
        ARCHIVED: "Job has been archived for long-term storage",
    }

    default_msg = messages.get(musical_status, f"Status: {musical_status}")

    # Add context-specific information if available
    if context:
        if "dependency" in context:
            return f"{default_msg}. Waiting for dependent job '{context['dependency']}' to complete"
        if "step" in context and "total_steps" in context:
            return (f"{default_msg}. Executing workflow step "
                    f"{context['step']}/{context['total_steps']}")
        if "error" in context:
            return f"{default_msg}. Error: {context['error']}"

    return default_msg


def is_terminal_status(status: str) -> bool:
    """
    Check if a status is terminal (no further transitions expected).

    Args:
        status: Status name (musical or legacy)

    Returns:
        True if the status is terminal, False otherwise
    """
    musical_status = translate_to_musical(status)
    terminal_statuses = {CODA, FINAL_NOTE, FAILED, DISSONANCE,
                         STOPPED, DELETED}
    return musical_status in terminal_statuses


def is_pre_submit_status(status: str) -> bool:
    """
    Check if a status is a pre-submit status (before backend execution).

    Args:
        status: Status name (musical or legacy)

    Returns:
        True if the status is pre-submit, False otherwise
    """
    musical_status = translate_to_musical(status)
    pre_submit_statuses = {SILENCE, PRELUDE, COMPOSING, ORCHESTRATING, TUNING}
    return musical_status in pre_submit_statuses


def is_execution_status(status: str) -> bool:
    """
    Check if a status is an execution status (during backend execution).

    Args:
        status: Status name (musical or legacy)

    Returns:
        True if the status is execution-related, False otherwise
    """
    musical_status = translate_to_musical(status)
    execution_statuses = {IN_MOVEMENT, FAILED, STOPPED}
    return musical_status in execution_statuses
