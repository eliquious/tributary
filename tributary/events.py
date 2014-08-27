# from . import core
from core import *
# import .core

__doc__ = """This submodules simply contains static messages and channel names"""

__all__ = ['DATA', 'STOP', 'START', 'KILL', 'StopMessage', 'StartMessage', 'KillMessage']

DATA = 'data'
STOP = 'tributary.stop'
START = 'tributary.start'
KILL = 'tributary.kill'

StopMessage = Message.create(STOP, True)
StartMessage = Message.create(START, True)
KillMessage = Message.create(KILL, True)
