"""Custom Logger

This module provides custom loggers.

"""

__author__ = "Simbu"
__date__ = "$May 29, 2022 1:28:14 PM$"

import os
import logging
import logging.handlers
import configparser

LEVELS = {f'DEBUG': logging.DEBUG,
          f'INFO': logging.INFO,
          f'WARNING': logging.WARNING,
          f'ERROR': logging.ERROR,
          f'CRITICAL': logging.CRITICAL
          }

LOGCONFIGFILE = os.environ['PROJECT_PATH'] + f"/config/logger.config"
LOG_PATH = os.environ['LOG_PATH']
# LOGCONFIGFILE = f"D:\\Initial_Version\\Lake_Ingestion_Codes\\config\\logger.config"


def getconfig(section=f'DEFAULT'):
    config = configparser.ConfigParser()
    config.read(LOGCONFIGFILE)
    return (config.get(section, f'logfile'),
            config.get(section, f'defaultlevel'),
            config.get(section, f'logmode'),
            )


def basiclogger(logname, section=f'DEFAULT', format_type=f"basic"):
    logfile, deflevel, logmode = getconfig(section)
    # This will help us to log messages in different log files at a time.
    logger = logging.getLogger(logname + '_' + section)
    logger.setLevel(LEVELS[deflevel])
    mode = {f'append': f'a', f'overwrite': f'w'}[logmode]
    logfile = os.path.join(LOG_PATH, logfile)
    if not logger.handlers:
        handler = logging.handlers.RotatingFileHandler(filename=logfile,
                                                       mode=mode,
                                                       maxBytes=0,
                                                       backupCount=0
                                                       )

        if format_type == f"extended":
            formatter = logging.Formatter(
                f"%(asctime)s :: %(levelname)s :: %(name)s ::  %(module)s  "
                f":: %(funcName)s :: %(lineno)s :: %(message)s")
        else:
            formatter = logging.Formatter(f'%(asctime)s :: %(levelname)s '
                                          f':: %(name)s :: %(message)s')

        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


if __name__ == f"__main__":
    print(f"Custom Logging Module")
    blogger = basiclogger(logname=f'default')
    blogger.info(f'This is a debug message')
