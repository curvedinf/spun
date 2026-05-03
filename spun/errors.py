class SpunError(RuntimeError):
    """
    Base class for Spun runtime errors.
    """


class UnknownCallError(SpunError, AttributeError):
    """
    Raised when application code calls a project function Spun has not loaded.
    """


class WorkNotFoundError(SpunError):
    """
    Raised when a durable work item cannot be found.
    """


class SerializationError(SpunError):
    """
    Raised when Spun cannot serialize durable work.
    """


class CallFailedError(SpunError):
    """
    Raised when a durable call finishes with an error.
    """


class CallTimeoutError(SpunError, TimeoutError):
    """
    Raised when waiting for a durable call exceeds the requested timeout.
    """
