#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tributary import *
from . import exceptions
from .utilities import validateType
import datetime, calendar, json
import gevent
from gevent import Greenlet
from gevent.queue import Queue, Empty
from gevent.pool import Group


__all__ = ['BasePredicate', 'BaseOverride', 'Message', 'BaseNode']

def deser(obj):
    """Default JSON serializer."""
    if isinstance(obj, datetime.datetime):
        if obj.utcoffset() is not None:
            obj = obj - obj.utcoffset()
        return obj.isoformat(' ')
        # millis = int(
        #     calendar.timegm(obj.timetuple()) * 1000 +
        #     obj.microsecond / 1000
        # )
        # return millis
    return obj


class BasePredicate(object):
    """BasePredicate is the parent class for all filters. Result of `apply` is evaluated for it's boolean value."""

    def apply(self, message):
        raise NotImplementedError("apply")

class BaseOverride(object):
    """BaseOverride is the parent class for all overrides."""

    def apply(self, message):
        """Overrides the message (or it's contents) with something else. This method should return the updated message."""
        raise NotImplementedError("apply")

class Message(object):
    """
    Message is the base class for transferring data from one node to another as well 
    as storing individual data elements (such as rows in a file).

    This class has several useful features. Every Message instance has a timestamp
    associated with it. The attributes of messages can be accessed via the 
    `getitem` and the `getattr` functions.
    """
    def __init__(self, **kwargs):
        super(Message, self).__init__()
        self.datetime = datetime.datetime.utcnow()
        self._source = None
        self._channel = 'data'
        self._forward = False

        # add properties at creation
        self.__dict__.update(**kwargs)

    @staticmethod
    def create(channel, forward=False, **kwargs):
        """Creates a new message with a given channel"""
        return Message(_channel=channel, _forward=forward, **kwargs)

    @property
    def channel(self):
        return self._channel
    @channel.setter
    def channel(self, value):
        self._channel = value

    @property
    def forward(self):
        return self._forward
    @forward.setter
    def forward(self, value):
        self._forward = value

    @property
    def utc(self):
        return self._utc

    @property
    def datetime(self):
        return self._datetime

    @datetime.setter
    def datetime(self, value):
        self._datetime = value
        self._utc = calendar.timegm(value.timetuple()) + value.microsecond / 1000000.

    @property
    def source(self):
        return self._source
    @source.setter
    def source(self, value):
        self._source = value    

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        for k, v in state.items():
            self.__dict__[k] = v
        return self

    def __delitem__(self, name):
        delattr(self, name)

    def __contains__(self, name):
        """Verifies that a parameter exists for the given message"""
        try:
            return hasattr(self, name) #stacktrace is currently suppressed
        except:
            tributary.log_exception("Message", "Attribute: %s" % name)
            exit()

    def get(self, name, default=None):
        """
        Returns a parameter if it exists, otherwise it raises an exception. Use the 'hasParam' to check if it exists.
        """
        if name in self:
            return getattr(self, name)
        else:
            return default

    def set(self, name, value):
        """Sets a parameter for this Message."""
        setattr(self, name, value)

    def update(self, **kwargs):
        """Updates the params from the given key-word arguments."""
        self.__dict__.update(kwargs)

    # def __dict__(self):
    #     """Returns a dictionary with all the parameters"""
    #     return self.__dict__

    def __iter__(self):
        return iter(self.__dict__)

    def keys(self):
        """Returns all the parameter names"""
        return self.__dict__.keys()

    def values(self):
        """Returns all the parameter values"""
        return self.__dict__.values()

    def items(self):
        """Returns all the parameter names and values in a list of tuples"""
        return self.__dict__.items()

    def __getitem__(self, param_name):
        return self.get(param_name)

    def __setitem__(self, name, value):
        self.set(name, value)

    def __str__(self):
        return json.dumps(dict(self), default=deser)

class BaseNode(Greenlet):
    """This is the base class for every node in the process tree. `BaseNode` manages the children of the various process nodes."""
    def __init__(self, name, verbose=False):
        super(BaseNode, self).__init__()
        self.inbox = Queue()
        self.name = name
        self.running = False
        self.verbose = verbose

        # stores results of node if required
        # self._state = Message()

        # child nodes
        self._children = {}

        # listeners
        self.listeners = {}

        # all 'normal' messages go through self.process
        self.on(events.DATA, self.process)

        # on node start, execute preProcess
        self.on(events.START, self.preProcess)
        self.on(events.START, lambda msg: setattr(self, 'running', True))

        # on node stop, set running to false, flush the queue and execute postProcess
        self.on(events.STOP, lambda msg: setattr(self, 'running', False))
        self.on(events.STOP, self.flush)
        self.on(events.STOP, self.postProcess)

        # on kill, call postProcess
        self.on(events.KILL, self.postProcess)

    def tick(self):
        """Yeilds the event loop to another node"""
        gevent.sleep(0)

    def sleep(self, seconds):
        """Makes the node sleep for the given seconds"""
        gevent.sleep(seconds)

    def stop(self):
        """Stop self and children"""
        self.handle(events.StopMessage)

    def start(self):
        """Starts the nodes"""
        for child in self.children:
            child.start()
        super(BaseNode, self).start()

    # @property
    # def state(self):
    #     """Returns the results from this processing element."""
    #     return self._state

    def clear(self):
        """Removes all children from this node"""
        self._children = {}

    def __len__(self):
        return len(self._children)

    def add(self, node, lazy=False):
        """Adds a child to the current node."""
        validateType("node", BaseNode, node)
        if not lazy:
            if node.name in self._children:
                raise Exception("Same name siblings are not allowed. Child node, %s, already exists." % node.name)
            self._children[node.name] = (len(self._children), node)
        else:
            name = str(node.name)
            num = 0
            while name in self:
                num += 1
                name = (node.name+"-"+str(num))
            node.name = name
            self._children[name] = (len(self._children), node)
        return self

    def __getitem__(self, name):
        """Returns the child with the name given. Raises NodeDoesNotExist exception if child does not exist."""
        if not name in self._children:
            raise exceptions.NodeDoesNotExist(name)
        return self._children[name][1]

    def __contains__(self, name):
        """
        Returns True if the node has the particular node.
        """
        return name in self._children

    @property
    def children(self):
        """Returns a list of chilren in the order they were added."""
        return (element for index, element in sorted(self._children.values()))

    def hasChildren(self):
        """Returns True if the node has any children."""
        return len(self._children) > 0

    def __delitem__(self, name):
        """Removes the child with the given name. Raises NodeDoesNotExist exception if the child does not exist."""
        if not name in self._children:
            raise exceptions.NodeDoesNotExist(name)
        del self._children[name]

    def __isub__(self, child):
        del self[child]
        return self

    def __iadd__(self, child):
        return self.add(child)

    def __str__(self):
        return "<%s: name=\"%s\">" % (self.__class__.__name__, self.name)

    def __iter__(self):
        return self.children

    def preProcess(self, message=None):
        """Pre processes the data source. Can be overriden."""
        pass

    def process(self, message=None):
        """This is the function which generates the results."""
        raise NotImplementedError("process")

    def postProcess(self, message=None):
        """Post processes the data source. Can be overriden.
        Assumes inbox has been flushed and should close any open files / connections.
        """
        pass

    def flush(self, message=None):
        """Empties the queue"""
        for message in self.inbox:
            self.handle(message)

    def execute(self):
        """Executes the preProcess, process, postProcess, scatter and gather methods"""
        self.running = True

        self.log("Starting...")
        while self.running:
            # self.log("Running...")
            try:
                message = self.inbox.get_nowait()
                self.handle(message)

                # yield to event loop after processing the message
                self.tick()

            except Empty:
                # Empty signifies that the queue is empty, so yield to another node
                self.tick()
                # pass

        self.log("Exiting...")

    def _run(self):
        self.execute()

    def scatter(self, message, forward=False):
        """Sends the results of this node to its children."""
        # if self.hasChildren():
        self.emit('data', message, forward)

    def emit(self, channel, message, forward=False):
        """The `emit` function can be used to send 'out-of-band' messages to 
        any child nodes. This essentially allows a node to send special messages 
        to any nodes listening."""
        # validateType('message', Message, message)
        message.channel = channel
        message.forward = forward
        if self.verbose:
            self.log("Sending message: %s on channel: %s" % (message, channel))
        for child in self.children:
            child.inbox.put_nowait(message)

        # yields to event loop
        self.tick()

    def emitBatch(self, channel, messages, forward=False):
        """Add all messages to child queues before yielding"""
        for message in messages:
            message.channel = channel
            message.forward = forward
            for child in self.children:
                child.inbox.put_nowait(message)

        # yields to event loop
        self.tick()

    def on(self, channel, function):
        """Registers event listeners based on a channel.
        Note: All functions registered should have 2 arguments. The first is current node (ie. self) and the second is the data being transferred.
        """
        if channel in self.listeners:
            self.listeners[channel].append(function)
        else:
            self.listeners[channel] = [function]

    def handle(self, message):
        """Handles events received on a given channel and forwards it if allowed."""

        # logging message
        if self.verbose:
            self.log("Processing message: %s" % message)

        if message.channel in self.listeners:
            for function in self.listeners[message.channel]:
                function(message)

        # forwards message if allowed
        if message.forward:
            self.log("Forwarding message on channel: %s" % message.channel)
            for child in self.children:
                child.inbox.put_nowait(message)

    def log(self, msg):
        """Logging capability is baked into every Node."""
        log_activity(str(self.name).upper(), msg, str(type(self).__name__))

class Engine(object):
    """docstring for Engine"""
    def __init__(self):
        super(Engine, self).__init__()
        self.nodes = []

    def _link(self, node):
        print node

    def add(self, node):
        """Adds a node to the engine to be executed"""
        validateType('node', BaseNode, node)
        # node.inbox.put(events.StartMessage)
        # node.link(self._link)
        self.nodes.append(node)
    
    def start(self):
        """Starts all the nodes"""
        for node in self.nodes:
            node.start()
            # node.inbox.put(events.StartMessage)
        gevent.joinall(self.nodes)

from . import events

# class BaseDataSource(BaseNode):
#     """docstring for DataSource"""
#     def __init__(self, name):
#         super(BaseDataSource, self).__init__(name)
#         reset()

#         # maintains process state
#         self.processed = False

#     def reset(self):
#         """Clears all filters, overrides and messages from the data source"""
#         self.state = Message(name=self.name, data=[], modifiers=[])

#     def __str__(self):
#         if self.processed:
#             return "<%s: %s>" % (self.__class__.__name__, self.name)
#         else:
#             return "<%s: %s (%s)>" % (self.__class__.__name__, self.name, "Not processed yet")

#     def addFilter(self, _filter):
#         """Adds a Filter for this specific data source"""
#         validateType("filter", BasePredicate, _filter)
#         self.modifiers.append(_filter)
#         return self

#     def addOverride(self, override):
#         """Adds an override"""
#         validateType("arg", BaseOverride, override)
#         self.modifiers.append(override)
#         return self

#     def addDataPoint(self, state):
#         """Adds a data point to this source if it is not filtered"""
#         validateType("state", DataPoint, state)

#         for modifier in self.modifiers:
#             if isinstance(modifier, BaseOverride):
#                 if modifier.isOverriden(state):
#                     state = modifier.override(state)
#             elif isinstance(modifier, BasePredicate):
#                 if not modifier.apply(state):
#                     return

#         # relates each statevector to this data source
#         state.source_name = self.name

#         #Adds the state vector only if the state has not been filtered
#         self.states.append(state)

#     def process(self, _input=None):
#         """This method must be overriden"""
#         raise NotImplementedYet("process")

#     def getDataPoints(self):
#         """Returns all the states for this data source"""
#         return self.states

#     def removeAllDataPoints(self):
#         self.states = []

#     def execute(self, _input=None):
#         """Executes the preProcess, process and postProcess methods"""
#         if not self.processed:
#             self.log("Processing...")
#             self.preProcess(_input)
#             self.process(_input)
#             self.postProcess(_input)
#             self.processed = True

#             self.scatter()
#             self.gather()
#             self.log("Exiting...")

#     def hasBeenProcessed(self):
#         """Returns whether this data source has been processed before"""
#         return self.processed

