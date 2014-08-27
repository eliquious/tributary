#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .utilities import validateType, strToUnixtime
from .exceptions import NotImplementedYet
from .core import BasePredicate, Message
import re
from math import *

__all__ = ["InclusiveTimePredicate", "ExclusiveTimePredicate", "PredicateGroup",
    "ReversePredicate", "AndPredicate", "OrPredicate",
    "ParamPredicate", "ParamEqualPredicate", "ParamNotEqualPredicate",
    "ParamInPredicate", "ParamNotInPredicate", "ParamContainsPredicate",
    "LessThanTimePredicate", "GreaterThanTimePredicate"]

class TimePredicate(BasePredicate):
    """TimePredicate: defines start and stop times for the filter. Expects arguments as datetime objects."""
    def __init__(self, start, stop):
        super(TimePredicate, self).__init__()
        self.start = start
        self.stop = stop

class InclusiveTimePredicate(TimePredicate):
    """InclusiveTimePredicate: If the message's time is `>= start and < stop` the message is valid. Expects arguments as datetime objects."""

    def apply(self, msg):
        validateType("msg", Message, msg)
        return msg.datetime >= self.start and msg.datetime < self.stop:

class ExclusiveTimePredicate(TimePredicate):
    """ExclusiveTimePredicate: If the message's time is `< start and >= stop` the message is valid. Expects arguments as datetime objects."""

    def apply(self, message):
        validateType("message", Message, message)
        return message.datetime < self.start and message.datetime >= self.stop:

class LessThanTimePredicate(BasePredicate):
    """Filters out every message whose timestamp is less than the given time. Expects argument as datetime objects."""
    def __init__(self, timestamp):
        self.timestamp = timestamp

    def apply(self, message):
        validateType("message", Message, message)
        return message.datetime > self.timestamp

class GreaterThanTimePredicate(BasePredicate):
    """Filters out every message vector whose timestamp is greater than the given time."""
    def __init__(self, timestamp):
        self.timestamp = timestamp

    def apply(self, message):
        validateType("message", Message, message)
        return message.time < self.unixtime


class PredicateGroup(BasePredicate):
    """docstring for PredicateGroup"""
    def __init__(self):
        super(PredicateGroup, self).__init__()
        self.filters = []

    def addPredicate(self, _filter):
        validateType("filter", BasePredicate, _filter)
        self.filters.append(_filter)
        return self

class AndPredicate(PredicateGroup):
    """AndPredicate: This filter combines several filters to find the logical AND of all the filters added"""

    def apply(self, message):
        apply = True

        for _filter in self.filters:
            if not _filter.apply(message):
                apply = False
                break
        return apply

class OrPredicate(PredicateGroup):
    """OrPredicate: This filter combines several filters to find the logical OR of all the filters added"""

    def apply(self, message):
        apply = False

        for _filter in self.filters:
            if _filter.apply(message):
                apply = True
                break
        return apply

class ReversePredicate(BasePredicate):
    """ReversePredicate simply reverses the output of the filter given in the constructor"""
    def __init__(self, _filter):
        super(ReversePredicate, self).__init__()
        self._filter = _filter

    def apply(self, message):
        return not self._filter.apply(message)

class ParamPredicate(BasePredicate):
    """docstring for ParamPredicate"""
    def __init__(self, param, validIfNotExist=True):
        super(ParamPredicate, self).__init__()
        validateType("validIfNotExist", bool, validIfNotExist)
        validateType("param", str, param)
        self.param = param
        self.validIfNotExist = validIfNotExist

    def eval(self, value):
        raise NotImplementedYet("eval")

    def apply(self, data):
        validateType("value", Message, data)
        if data.hasParam(self.param):
            return not self.eval(data.getParam(self.param))
        else:
            return self.validIfNotExist

class ParamEqualPredicate(ParamPredicate):
    """If a data point has the given parameter and is equal to
       the value supplied, it is filtered out.
       """
    def __init__(self, param, value, validIfNotExist=True):
        super(ParamEqualPredicate, self).__init__(param, validIfNotExist)
        self.value = value

    def eval(self, value):
        return value == self.value

class ParamNotEqualPredicate(ParamEqualPredicate):
    """If a data point has the given parameter and is not equal to
       the value supplied, it is filtered out.
       """

    def eval(self, value):
        return value != self.value

class ParamLessThanPredicate(ParamEqualPredicate):
    """If a data point has the given parameter and is less than
       the value supplied, it is filtered out.
       """

    def eval(self, value):
        return value < self.value

class ParamLessThanOrEqualToPredicate(ParamEqualPredicate):
    """If a data point has the given parameter and is less than or equal to
       the value supplied, it is filtered out.
       """

    def eval(self, value):
        return value <= self.value

class ParamGreaterThanPredicate(ParamEqualPredicate):
    """If a data point has the given parameter and is greater than
       the value supplied, it is filtered out.
       """

    def eval(self, value):
        return value > self.value

class ParamGreaterThanOrEqualToPredicate(ParamEqualPredicate):
    """If a data point has the given parameter and is greater than or equal to
       the value supplied, it is filtered out.
       """

    def eval(self, value):
        return value >= self.value

class ParamInPredicate(ParamEqualPredicate):
    """If a data point has the given parameter and is in
       the value supplied, it is filtered out.
       """

    def eval(self, value):
        return value in self.value

class ParamNotInPredicate(ParamEqualPredicate):
    """If a data point has the given parameter and is not in
       the value supplied, it is filtered out.
       """

    def eval(self, value):
        return value not in self.value

class FilePredicate(BasePredicate):
    """FilePredicates are meant to filter file names which are output from a RecursiveFileDataSource.
       They can be used to limit the file names returned."""
    def __init__(self, re_pattern):
        super(FilePredicate, self).__init__()
        self.pattern = re_pattern

    def apply(self, value):
        validateType("value", Message, value)
        filename = value.getParam("filename")
        return re.search(self.pattern, filename) is not None

class ParamContainsPredicate(ParamPredicate):
    """ParamContainsPredicate are meant to filter file names which are output from a RecursiveFileDataSource.
       They can be used to limit the file names returned."""
    def __init__(self, param, contains):
        super(ParamContainsPredicate, self).__init__(param, validIfNotExist=False)
        self.contains = contains

    def eval(self, value):
        return not self.contains in value
