"""
Spun.

Durable execution for Wove-shaped Python work.
"""

from .calls import call
from .promise import CallPromise
from .runtime import config
from .schedule import Calendar, Cron, schedule
from .worker import Worker
from .wove import SpunEnvironmentExecutor, register

register()

__version__ = "0.1.0"
__all__ = [
    "__version__",
    "call",
    "schedule",
    "Cron",
    "Calendar",
    "CallPromise",
    "Worker",
    "SpunEnvironmentExecutor",
    "config",
]
