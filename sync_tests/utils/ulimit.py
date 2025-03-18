import logging
import resource

LOGGER = logging.getLogger(__name__)

# fmt: off
ulimits = {
    "RLIMIT_CPU": resource.RLIMIT_CPU,               # CPU time in seconds
    "RLIMIT_FSIZE": resource.RLIMIT_FSIZE,           # Max file size
    "RLIMIT_DATA": resource.RLIMIT_DATA,             # Max data segment size
    "RLIMIT_STACK": resource.RLIMIT_STACK,           # Max stack size
    "RLIMIT_CORE": resource.RLIMIT_CORE,             # Max core file size
    "RLIMIT_RSS": resource.RLIMIT_RSS,               # Max resident set size (deprecated)
    "RLIMIT_NPROC": resource.RLIMIT_NPROC,           # Max number of processes
    "RLIMIT_NOFILE": resource.RLIMIT_NOFILE,         # Max open files
    "RLIMIT_MEMLOCK": resource.RLIMIT_MEMLOCK,       # Max memory lockable
    "RLIMIT_AS": resource.RLIMIT_AS,                 # Max virtual memory size
    "RLIMIT_SIGPENDING": resource.RLIMIT_SIGPENDING, # Max number of pending signals
    "RLIMIT_MSGQUEUE": resource.RLIMIT_MSGQUEUE,     # Max bytes in POSIX message queues
}
# fmt: on


def _get_val_str(val: int) -> str:
    if val == resource.RLIM_INFINITY:
        return "unlimited"
    return str(val)


def print_ulimit() -> None:
    """Print the current ulimit settings."""
    for name, limit in ulimits.items():
        soft_int, hard_int = resource.getrlimit(limit)
        soft = _get_val_str(soft_int)
        hard = _get_val_str(hard_int)
        LOGGER.info(f"{name}: soft={soft}, hard={hard}")
