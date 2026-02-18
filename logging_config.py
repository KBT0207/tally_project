import logging
import logging.config
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime
import sys
import io

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

today_date = datetime.now().strftime("%d-%b-%Y")

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s"
        }
    },
    "handlers": {
        "console_handler": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
        "file_handler": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "level": "INFO",
            "formatter": "standard",
            "filename": os.path.join(LOG_DIR, f"main_{today_date}.log"),
            "when": "midnight",
            "interval": 1,
            "backupCount": 30,
            "encoding": "utf-8",
        },
        "error_file_handler": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "level": "ERROR",
            "formatter": "standard",
            "filename": os.path.join(LOG_DIR, f"error_{today_date}.log"),
            "when": "midnight",
            "interval": 1,
            "backupCount": 30,
            "encoding": "utf-8",
        },
    },
    "root": {
        "level": "INFO",
        "handlers": [
            "console_handler",
            "file_handler",
            "error_file_handler",
        ],
    },
}

logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger("main")
