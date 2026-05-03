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
