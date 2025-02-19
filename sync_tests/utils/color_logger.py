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


class ColorLogger(logging.Logger):
    def __init__(self, name: str) -> None:
        super().__init__(name, logging.INFO)
        color_formatter = ColorFormatter("%(message)s")
        console = logging.StreamHandler()
        console.setFormatter(color_formatter)
        self.addHandler(console)
