import logging
import logging.config
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)

today_date = datetime.now().strftime('%d-%b-%Y')

# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s'
        }
    },
    'handlers': {
        'console_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO'
        },
        'file_handler': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': f'logs/main_{today_date}.log',
            'when': 'midnight', 
            'interval': 1,
            'backupCount': 30,
            'formatter': 'standard',
            'level': 'INFO'
        }
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['console_handler', 'file_handler'],
            'level': 'INFO',
            'propagate': False
        }
    }
}

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("main")
