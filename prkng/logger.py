# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com

Custom Logger inspired by the kivy Logger module
"""
import logging

Logger = logging.getLogger('prkng')

BLACK, RED, GREEN, YELLOW, BLUE, CYAN, WHITE = list(range(7))

COLORS = {
    'CRITICAL': RED,
    'ERROR': RED,
    'WARNING': YELLOW,
    'INFO': GREEN,
    'DEBUG': CYAN,
}

LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

COLOR_SEQ = "\033[1;%dm"
RESET_SEQ = "\033[0m"
BOLD_SEQ = "\033[1m"


def formatter_message(message, use_color=True):
    if use_color:
        message = message.replace("$RESET", RESET_SEQ)
        message = message.replace("$BOLD", BOLD_SEQ)
    else:
        message = message.replace("$RESET", "").replace("$BOLD", "")
    return message


class ColoredFormatter(logging.Formatter):

    def __init__(self, msg, use_color=True):
        logging.Formatter.__init__(self, msg)
        self.use_color = use_color

    def format(self, record):
        try:
            msg = record.msg.split(':', 1)
            if len(msg) == 2:
                record.msg = '[%-12s]%s' % (msg[0], msg[1])
        except:
            pass
        levelname = record.levelname
        if self.use_color and levelname in COLORS:
            levelname_color = (
                COLOR_SEQ % (30 + COLORS[levelname]) + levelname + RESET_SEQ)
            record.levelname = levelname_color
        return logging.Formatter.format(self, record)


console = logging.StreamHandler()
color_fmt = formatter_message('[%(asctime)s][%(levelname)-18s][%(module)s] %(message)s')
formatter = ColoredFormatter(color_fmt, use_color=True)
console.setFormatter(formatter)
Logger.addHandler(console)


def set_level(level='info'):
    """
    Set log level
    """
    Logger.setLevel(LOG_LEVELS.get(level))
