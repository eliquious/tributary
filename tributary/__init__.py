#!/usr/bin/env python
# -*- coding: utf-8 -*-

__doc__ = 'Tributary is a modular data processing framework.'

try:
    from ._version import __version__, __revision__
except ImportError:
    __version__ = "UNKNOWN"
    __revision__ = "UNKNOWN"

import logging, sys

__all__ = ['log_script_activity', 'log_exception', 'log_info', 'log_debug', 'log_warning', 'log_error', 'log_critical', 'log_activity']

# create logger
logger = logging.getLogger('tributary')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler(stream=sys.stdout)
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter(fmt="%(asctime)s.%(msecs)d - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


def log_activity(producer, msg, category):
    """
    Logs information regarding the specific category.

    :param producer: name of script or node producing the log output
    :type producer: String
    :param msg: Message to log
    :type msg: String
    :param category: logging category
    :type category: String

    """
    ltype = str(category).upper()
    alias = str(producer).upper()
    
    if 'INFO' in ltype:
        logger.info("%s [%s] - %s", ltype, alias, msg)
    elif 'DEBUG' in ltype:
        logger.debug("%s [%s] - %s", ltype, alias, msg)
    elif 'ERROR' in ltype:
        logger.error("%s [%s] - %s", ltype, alias, msg)
    elif 'WARNING' in ltype:
        logger.warning("%s [%s] - %s", ltype, alias, msg)
    elif 'CRITICAL' in ltype:
        logger.critical("%s [%s] - %s", ltype, alias, msg)
    elif 'EXCEPTION' in ltype:
        logger.exception("%s [%s] - %s", ltype, alias, msg)
    else:
        logger.info("%s [%s] - %s", ltype, alias, msg)


def log_script_activity(script_alias, msg):
    """
    Logs additional script information not included in traditional execution output.

    :param script_alias: name of script or node producing output
    :type script_alias: String
    :param msg: message to be logged
    :type msg:  String
    :rtype: None

    Usage::

        import tributary
        SCRIPT_NAME = "Power Station"

        # 2012-08-02 14:00:43.816 - SCRIPT [POWER STATION] - Welcome to 'Defend the Reactor'
        tributary.log_script_activity(SCRIPT_NAME, "Welcome to 'Defend the Reactor'")

    """
    log_activity(script_alias, msg, 'SCRIPT')


def log_info(script_alias, msg):
    """
    Logs general information.

    :param script_alias: name of script or node producing output
    :type script_alias: String
    :param msg: message to be logged
    :type msg:  String
    :rtype: None

    Usage::

        import tributary
        SCRIPT_NAME = "Power Station"

        # 2012-08-02 14:00:43.816 - INFO [POWER STATION] - Reactor Levels Normal
        tributary.log_info(SCRIPT_NAME, "Reactor Levels Normal")

    """
    log_activity(script_alias, msg, 'INFO')

def log_debug(script_alias, msg):
    """
    Logs debug information.

    :param script_alias: name of script or node producing output
    :type script_alias: String
    :param msg: message to be logged
    :type msg:  String
    :rtype: None

    Usage::

        import tributary
        SCRIPT_NAME = "Power Station"

        # 2012-08-02 14:00:43.816 - DEBUG [POWER STATION] - Debug Mode Engaged
        tributary.log_debug(SCRIPT_NAME, "Debug Mode Engaged")

    """
    log_activity(script_alias, msg, 'DEBUG')

def log_warning(script_alias, msg):
    """
    Logs warning information.

    :param script_alias: name of script or node producing output
    :type script_alias: String
    :param msg: message to be logged
    :type msg:  String
    :rtype: None

    Usage::

        import tributary
        SCRIPT_NAME = "Power Station"

        # 2012-08-02 14:00:43.816 - WARNING [POWER STATION] - DON'T PRESS THE RED BUTTON
        tributary.log_warning(SCRIPT_NAME, "DON'T PRESS THE RED BUTTON")

    """
    log_activity(script_alias, msg, 'WARNING')

def log_error(script_alias, msg, terminate=False):
    """
    Logs error information. If you want to log an exception use 'log_exception' instead.

    :param script_alias: name of script or node producing output
    :type script_alias: String
    :param msg: message to be logged
    :type msg:  String
    :rtype: None

    Usage::

        import tributary
        SCRIPT_NAME = "Power Station"

        # 2012-08-02 14:00:43.816 - ERROR [POWER STATION] - Intruder Alert!
        tributary.log_error(SCRIPT_NAME, "Intruder Alert!")

    """
    log_activity(script_alias, msg, 'ERROR')
    if terminate:
        exit(1)

def log_critical(script_alias, msg, terminate=False):
    """
    Logs critical error information. If you want to log an exception use 'log_exception' instead.

    :param script_alias: name of script or node producing output
    :type script_alias: String
    :param msg: message to be logged
    :type msg:  String
    :rtype: None

    Usage::

        import tributary
        SCRIPT_NAME = "Power Station"

        # 2012-08-02 14:00:43.816 - CRITICAL [POWER STATION] - WHY DID YOU PRESS THE RED BUTTON?!
        tributary.log_critical(SCRIPT_NAME, "WHY DID YOU PRESS THE RED BUTTON?!")

    """
    log_activity(script_alias, msg, 'CRITICAL')
    if terminate:
        exit(1)

def log_exception(script_alias, msg, terminate=False):
    """
    Logs exception information.

    :param script_alias: name of script or node producing output
    :type script_alias: String
    :param msg: message to be logged
    :type msg:  String
    :rtype: None

    Usage::

        import tributary
        SCRIPT_NAME = "Power Station"

        # exception
        # 2012-08-02 14:00:43.816 - EXCEPTION [POWER STATION] - BOOM
        # Traceback (most recent call last):
        #   File "/reactor/main.py", line 10001, in <module>
        #     raise Exception("to the rule")
        # Exception: to the rule
        try:
            raise Exception("to the rule")
        except:
            log_exception(SCRIPT_NAME, "Exception Test")

    """
    log_activity(script_alias, msg, 'EXCEPTION')
    if terminate:
        exit(1)



if __name__ == '__main__':

    SCRIPT_NAME = "Power Station"

    # 2012-08-02 12:22:57.677 - SCRIPT [LOG TESTING] - Starting
    log_script_activity(SCRIPT_NAME, "Welcome to 'Defend the Reactor'")

    # custom
    # 2012-08-02 12:22:57.677 - CUSTOM [LOG TESTING] - Other Statement
    log_activity(SCRIPT_NAME, "Radiation Levels Normal", "Reactor")

    # debug
    log_debug(SCRIPT_NAME, "Debug Mode Engaged")

    # error
    # 2012-08-02 12:22:57.677 - ERROR [LOG TESTING] - Error Statement
    log_error(SCRIPT_NAME, "Intruder Alert!")

    # warning
    # 2012-08-02 12:22:57.677 - WARNING [LOG TESTING] - DON'T PRESS THE RED BUTTON
    log_warning(SCRIPT_NAME, "DON'T PRESS THE RED BUTTON")

    # critical
    # 2012-08-02 12:22:57.677 - CRITICAL [LOG TESTING] - WHY DID YOU PRESS THE RED BUTTON?!
    log_critical(SCRIPT_NAME, "WHY DID YOU PRESS THE RED BUTTON?!")

    try:
        raise Exception("to the rule")
    except Exception, e:
        log_exception(SCRIPT_NAME, "BOOM")

    # 2012-08-02 12:22:57.677 - SCRIPT [LOG TESTING] - Finished
    log_script_activity(SCRIPT_NAME, "Game Over")

