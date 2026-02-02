import logging
import typing as tp

import colorama


class ColorFormatter(logging.Formatter):
    COLORS: tp.ClassVar[dict[str, str]] = {
        "WARNING": colorama.Fore.YELLOW,
        "ERROR": colorama.Fore.RED,
        "DEBUG": colorama.Fore.BLUE,
        # Keep "INFO" uncolored
        "CRITICAL": colorama.Fore.RED,
    }

    def format(self, record: logging.LogRecord) -> str:
        color: str | None = self.COLORS.get(record.levelname)
        if color:
            record.name = f"{color}{record.name}{colorama.Style.RESET_ALL}"
            record.levelname = f"{color}{record.levelname}{colorama.Style.RESET_ALL}"
            record.msg = f"{color}{record.msg}{colorama.Style.RESET_ALL}"
        return super().format(record)


def configure_logging(fmt: str | None = None) -> None:
    fmt = fmt or "%(message)s"
    logging.setLoggerClass(logging.Logger)  # Ensure standard logger is used
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)  # Set the global log level

    # Remove existing handlers to avoid duplicates
    while root_logger.handlers:
        root_logger.handlers.pop()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColorFormatter(fmt))
    root_logger.addHandler(console_handler)
