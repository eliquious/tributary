#!/usr/bin/env python
# -*- coding: utf-8 -*-

__doc__ = 'Tributary is a modular data processing framework.'

try:
    from ._version import __version__, __revision__
except ImportError:
    __version__ = "UNKNOWN"
    __revision__ = "UNKNOWN"

# import logging, sys

from .log import *
import ext
