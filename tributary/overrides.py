#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Overrides and Bias classes for tributary
from .core import BaseOverride
import operators

__all__ = ['TimeBiasOverride', 'StaticParamOverride', 'CopyParamOverride', \
    'ParamTypeOverride', 'RenameParamOverride', 'FunctionOverride', \
    'OperatorOverride', 'MultiplyParamOverride', 'AddParamOverride', \
    'SubtractParamOverride', 'DivideParamOverride', 'StringTypeOverride', \
    'FloatTypeOverride', 'IntTypeOverride']

class TimeBiasOverride(BaseOverride):
    """Adds a time bias to messages. Bias should be a timedelta."""
    def __init__(self, bias):
        super(TimeBiasOverride, self).__init__()
        self.bias = bias

    def apply(self, msg):
        msg.datetime = msg.datetime + self.bias
        return msg

class StaticParamOverride(BaseOverride):
    """Adds a static reference position to every message"""
    def __init__(self, paramName, value):
        super(StaticParamOverride, self).__init__()
        self.paramName = paramName
        self.value = value

    def apply(self, message):
        message.setParam(self.paramName, self.value)
        return message

class CopyParamOverride(BaseOverride):
    """docstring for ParamTypeOverride"""
    def __init__(self, param, new_param):
        super(CopyTimeOverride, self).__init__()
        self.param = param
        self.newparam = newparam

    def apply(self, message):
        if self.param in message:
            message[self.newparam] = message[self.param]
        return message


class RenameParamOverride(BaseOverride):
    """docstring for RenameParamOverride"""
    def __init__(self, old, repl):
        super(RenameParamOverride, self).__init__()
        self.param = old
        self.repl = repl

    def apply(self, message):
        if self.param in message:
            message[self.repl] = message[self.param]
            del message[self.param]
        return message

class FunctionOverride(BaseOverride):
    """docstring for FunctionOverride"""
    def __init__(self, param, fn):
        super(FunctionOverride, self).__init__()
        self.param = param
        self.fn = fn

    def apply(self, message):
        if self.param in message:
            message[self.param] = self.fn(message[self.param])
        return message

class StringTypeOverride(FunctionOverride):
    """docstring for StringTypeOverride"""
    def __init__(self, param):
        super(StringTypeOverride, self).__init__(param, str)

class FloatTypeOverride(FunctionOverride):
    """docstring for FloatTypeOverride"""
    def __init__(self, param):
        super(FloatTypeOverride, self).__init__(param, float)

class IntTypeOverride(FunctionOverride):
    """docstring for IntTypeOverride"""
    def __init__(self, param):
        super(IntTypeOverride, self).__init__(param, int)

class OperatorOverride(BaseOverride):
    """docstring for OperatorOverride"""
    def __init__(self, param, mod, op):
        super(OperatorOverride, self).__init__()
        self.param = param
        self.mod = mod
        self.op = op
    
    def apply(self, message):
        if self.param in message:
            message[self.param] = self.op(message[self.param], self.mod)
        return message

class MultiplyParamOverride(OperatorOverride):
    """docstring for ParamTypeOverride"""
    def __init__(self, param, num):
        super(MultiplyParamOverride, self).__init__(param, num, operators.mul)

class AddParamOverride(OperatorOverride):
    """docstring for ParamTypeOverride"""
    def __init__(self, param, num):
        super(AddParamOverride, self).__init__(param, num, operators.add)

class SubtractParamOverride(OperatorOverride):
    """docstring for ParamTypeOverride"""
    def __init__(self, param, num):
        super(SubtractParamOverride, self).__init__(param, num, operators.sub)

class DivideParamOverride(OperatorOverride):
    """docstring for ParamTypeOverride"""
    def __init__(self, param, num):
        super(DivideParamOverride, self).__init__(param, num, operators.div)

