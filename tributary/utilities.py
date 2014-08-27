# Utility classes and functions for tributary
import calendar, time
from time import gmtime
from datetime import datetime

__all__ = ["validateType", "Enum", "strToUnixtime", \
    "validateNotNone", "validateIn", "validateIter", \
    "unixtimeToDatetime", "hmsToSAM"]

def validateType(varname, correct_type, value):
    """Validates the type of an object"""
    if not isinstance(value, correct_type):
        raise TypeError("Variable '%s' must be of type '%s'; not %s" % (varname, correct_type, type(value)))
    return

def validateNotNone(varname, value):
    """Validates that the object is not none"""
    if value is None:
        raise ValueError("Variable '%s' must not be None" % (varname, value))
    return

def validateIn(varname, value, acceptable):
    """Validates that the object is in the list 'acceptable'"""
    if not value in acceptable:
        raise ValueError("Variable '%s' must be in %s; received '%s'" % (varname, str(acceptable), value))
    return

def validateIter(varname, value):
    """Validates that the object is iterable"""
    if hasattr(value, '__iter__'):
        raise ValueError("Variable '%s' must be iterable; received '%s'" % (varname, value))
    return

class Enum(object):
    """Enumeration class"""
    def __init__(self, *args, **kwargs):
        super(Enum, self).__init__()
        self.__dict__.update(dict(zip(args, args), **kwargs))

def strToUnixtime(date_string, format):
    """Converts a date string to unixtime based on the input format"""
    dt = datetime.strptime(date_string, format)
    unix = float("%0.6f" % (calendar.timegm(dt.utctimetuple()) + dt.time().microsecond / 1000000.0))
    return unix

def unixtimeToDatetime(unixtime, format="%Y-%m-%d %H:%M:%S.%f"):
    """Converts a unixtime float to a string formatted datetime"""
    timestamp_unix = unixtime
    timestamp_micro = int(10**6 * (timestamp_unix - int(timestamp_unix))), #trailing comma is needed to make this a tuple
    timestamp_params = gmtime(timestamp_unix)[:6] + timestamp_micro
    timestamp_obj = datetime(*timestamp_params)
    return timestamp_obj.strftime(format)

def hmsToSAM(hms):
    """Converts an HH:MM:SS to seconds after midnight"""
    h, m, s = [int(t) for t in hms.split(":")]
    return h * 3600 + m * 60 + s
