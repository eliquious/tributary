#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tributary import *
from . import exceptions
from .log import *
from .utilities import validateType
import datetime, calendar, json
import gevent
from gevent import Greenlet
from gevent.queue import Queue, Empty
from gevent.pool import Group

__all__ = ['BasePredicate', 'BaseOverride', 'Message', 'Actor', 'Engine', 'ExecutionContext', 'Service', 'SynchronousActor']

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
    elif isinstance(obj, MessageContent):
        return obj.__dict__
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


class MessageContent(object):
    """docstring for MessageContent"""
    def __init__(self, **kwargs):
        super(MessageContent, self).__init__()
        self.__dict__.update(**kwargs)

    def __delitem__(self, name):
        delattr(self, name)

    def __contains__(self, name):
        """Verifies that a parameter exists for the given message"""
        try:
            return hasattr(self, name) #stacktrace is currently suppressed
        except:
            log_exception("Message", "Attribute: %s" % name)
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
        keys = ', '.join([('%s=%s' % (k, v)) for k, v in self.items()])
        return 'Message(%s)' % keys.encode('string-escape')
        # return json.dumps(dict(self), default=deser)

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
        # self._source = None
        self._channel = 'data'
        self._forward = False

        # add properties at creation
        self.data = MessageContent(**kwargs)

    @staticmethod
    def create(channel, forward=False, **kwargs):
        """Creates a new message with a given channel"""
        msg = Message(**kwargs)
        msg.channel = channel
        msg.forward = forward
        return msg

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

    # @property
    # def source(self):
    #     return self._source
    # @source.setter
    # def source(self, value):
    #     self._source = value    

    # def __getstate__(self):
    #     return self.__dict__

    # def __setstate__(self, state):
    #     for k, v in state.items():
    #         self.__dict__[k] = v
    #     return self

    def __iter__(self):
        return iter(self.__dict__)

    def __repr__(self):
        return '%s(channel=%s, datetime=%s, forward=%s, data=%s)' % (
            type(self).__name__,
            self.channel,
            self.datetime, 
            self.forward,
            self.data)
        # return json.dumps((self.__dict__), default=deser)

class Actor(Greenlet):
    """This is the base class for every node in the process tree. `Actor` manages the children of the various process nodes."""
    def __init__(self, name):
        super(Actor, self).__init__()
        self.inbox = Queue()
        self.name = name
        self.running = False
        self._context = None

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
        self.on(events.STOP, self.flush)
        self.on(events.STOP, self.postProcess)
        # self.on(events.STOP, lambda msg: setattr(self, 'running', False))
        # self.on(events.STOP, self.kill)

        # on kill, call postProcess
        self.on(events.KILL, self.postProcess)
        self.on(events.KILL, lambda msg: setattr(self, 'running', False))
        # self.on(events.KILL, self.kill)

        # listen to exceptions
        self.link_exception(self.handleException)

    def setContext(self, ctx):
        """Sets the execution context"""
        ctx.addActor(self)
        self._context = ctx
        for child in self.children:
            child.setContext(ctx)

    def getContext(self):
        """Gets the execution context"""
        return self._context

    def tick(self):
        """Yields the event loop to another node"""
        # self.log_trace("Yielding...")
        gevent.sleep(0)

    def sleep(self, seconds):
        """Makes the node sleep for the given seconds"""
        self.log_trace("Sleeping %ss..." % seconds)
        gevent.sleep(seconds)

    def handleException(self, exc):
        pass

    def stop(self):
        """Stop self and children"""
        self.handle(events.StopMessage)
        # gevent.joinall(list(self.children))
        # self.tick()

    def start(self):
        """Starts the nodes"""
        if not self.running:
            self.handle(events.StartMessage)
            for child in self.children:
                child.start()
            super(Actor, self).start()

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
        validateType("node", (Actor, SynchronousActor), node)
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

        # gevent.joinall(self.dependencies)
        # waiting = True
        # while waiting:
        #     waiting = False
        #     for dep in self.dependencies:
        #         if dep.running:
        #             waiting = True
        #     self.tick()

        self.log_debug("Flushing Queue...")
        if not self.inbox.empty():
            for message in self.inbox:
                self.handle(message)
        self.running = False

    def execute(self):
        """Executes the preProcess, process, postProcess, scatter and gather methods"""
        self.running = True

        self.log_info("Starting...")
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

        # self.tick()
        # self.stop()
        self.log_info("Exiting...")

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
        validateType('message', Message, message)
        message.channel = channel
        message.forward = forward
        # message.source = self
        self.log_debug("Sending message: %s on channel: %s" % (message, channel))
        for child in self.children:
            child.insert(message)
            # child.inbox.put_nowait(message)

        # yields to event loop
        self.tick()

    def emitBatch(self, channel, messages, forward=False):
        """Add all messages to child queues before yielding"""
        for message in messages:
            message.channel = channel
            message.forward = forward
            # message.source = self
            self.log_debug("Sending message: %s on channel: %s" % (message, channel))
            for child in self.children:
                # child.inbox.queue.extend(messages)
                child.insert(message)

        # for child in self.children:
            # child.inbox.queue.extend(messages)

        # yields to event loop
        self.tick()

    def removeListener(self, channel, function):
        """Removes a listener function from a channel"""
        if channel in self.listeners:
            try:
                self.listeners[channel].remove(function)
                return True
            except ValueError:
                return False
        else:
            return False

    def on(self, channel, function):
        """Registers event listeners based on a channel.
        Note: All functions registered should have 2 arguments. The first is current node (ie. self) and the second is the data being transferred.
        """
        self.log_trace("Registering function on channel '%s'" % (channel))
        if channel in self.listeners:
            self.listeners[channel].append(function)
        else:
            self.listeners[channel] = [function]

    def handle(self, message):
        """Handles events received on a given channel and forwards it if allowed."""

        # logging message
        # if self.verbose:
        # self.log("Processing message: %s" % message)

        if message.channel in self.listeners:
            for function in self.listeners[message.channel]:
                # print function
                function(message)

        # forwards message if allowed
        if message.forward:
            self.log_trace("Forwarding message on channel: %s" % message.channel)
            for child in self.children:
                child.insert(message)
                # child.handle(message)
                # child.inbox.put_nowait(message)
            # self.tick()

    def insert(self, message):
        """Inserts a new message to be handled"""
        self.inbox.put_nowait(message)

    def log(self, msg):
        """Logging capability is baked into every Node."""
        self.log_info(msg)

    def log_debug(self, msg):
        """Logs a DEBUG message"""
        log_debug(str(self.name).upper(), msg)

    def log_info(self, msg):
        """Logs an INFO message"""
        log_info(str(self.name).upper(), msg)

    def log_warning(self, msg):
        """Logs a WARNING message"""
        log_warning(str(self.name).upper(), msg)

    def log_error(self, msg):
        """Logs an ERROR message"""
        log_error(str(self.name).upper(), msg)

    def log_critical(self, msg):
        """Logs a CRITICAL message"""
        log_critical(str(self.name).upper(), msg)

    def log_exception(self, msg):
        """Logs an exception"""
        log_exception(str(self.name).upper(), msg)

    def log_trace(self, msg):
        """Logs low-level debug message"""
        log_trace(str(self.name).upper(), msg)


class Engine(object):
    """docstring for Engine"""
    def __init__(self, ctx=None):
        super(Engine, self).__init__()
        self.nodes = []

        # set execution context
        if ctx:
            self._context = ctx
        else:
            self._context = ExecutionContext()

    def _link(self, node):
        print node

    def add(self, node):
        """Adds a node to the engine to be executed"""
        validateType("node", (Actor, SynchronousActor), node)
        node.setContext(self._context)
        self.nodes.append(node)
    
    def start(self):
        """Starts all the nodes"""
        start = datetime.datetime.now()
        log_script_activity("Engine", "Engine started...")
        for node in self.nodes:
            node.start()
        try:
            gevent.joinall(self.nodes)
        except (KeyboardInterrupt, SystemExit):
            log_script_activity("Engine", "Ctrl-C: Stopping processes")

        # signal stop
        for node in self.nodes:
            node.stop()

        # joining all the services
        for name, svc in self._context.services.items():
            log_script_activity("Engine", "Joining: %s" % name)
            svc.join()

        elapsed = datetime.datetime.now() - start
        log_script_activity("Engine", "Elapsed: %s" % elapsed)

        # for node in self.nodes:
        #     gevent.joinall(list(node.children))

class ExecutionContext(object):
    """docstring for ExecutionContext"""
    def __init__(self):
        super(ExecutionContext, self).__init__()
        self.actors = {}
        self.services = {}

    def addActor(self, actor):
        """Adds an actor to the execution context"""
        if isinstance(actor, (Actor, SynchronousActor)):
            self.actors[actor.name] = actor
        else:
            raise Exception("Not an actor: " + str(actor))

    def sendTo(self, actor_name, channel, msg, forward=False):
        """Allows sending a message to another actor which is not directly listening"""
        if actor_name in self.actors:
            self.actors[actor_name].handle(Message.create(channel, forward, **dict(msg.data.items())))
            # self.actors[actor_name].emit(channel, msg, forward)
        else:
            raise Exception("Actor not found: " + actor_name)

    def addService(self, srv):
        """Adds a service to the context. Services are not actors. They are meant to be used if global state needs to be maintained."""
        if isinstance(srv, Service):
            self.services[srv.name] = srv
        else:
            raise Exception("Not a service: " + str(actor))

    def getService(self, name):
        """Returns a service reference"""
        if name in self.services:
            return self.services[name]
        else:
            raise Exception("Service not found: " + name)

class Service(object):
    """docstring for Service"""
    def __init__(self, name):
        super(Service, self).__init__()
        self.name = name

    def join(self):
        pass

    def log(self, msg):
        """Logging capability is baked into every Node."""
        self.log_info(msg)

    def log_debug(self, msg):
        """Logs a DEBUG message"""
        log_debug(str(self.name).upper(), msg)

    def log_info(self, msg):
        """Logs an INFO message"""
        log_info(str(self.name).upper(), msg)

    def log_warning(self, msg):
        """Logs a WARNING message"""
        log_warning(str(self.name).upper(), msg)

    def log_error(self, msg):
        """Logs an ERROR message"""
        log_error(str(self.name).upper(), msg)

    def log_critical(self, msg):
        """Logs a CRITICAL message"""
        log_critical(str(self.name).upper(), msg)

    def log_exception(self, msg):
        """Logs an exception"""
        log_exception(str(self.name).upper(), msg)

    def log_trace(self, msg):
        """Logs low-level debug message"""
        log_trace(str(self.name).upper(), msg)


class SynchronousActor(object):
    """This is the base class for any nodes in the process tree which need to execute synchronously."""
    def __init__(self, name):
        super(SynchronousActor, self).__init__()
        self.name = name
        self.running = False
        self._context = None

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
        self.on(events.STOP, self.flush)
        self.on(events.STOP, self.postProcess)
        # self.on(events.STOP, lambda msg: setattr(self, 'running', False))
        # self.on(events.STOP, self.kill)

        # on kill, call postProcess
        self.on(events.KILL, self.postProcess)
        self.on(events.KILL, lambda msg: setattr(self, 'running', False))
        # self.on(events.KILL, self.kill)

        # listen to exceptions
        # self.link_exception(self.handleException)

    def setContext(self, ctx):
        """Sets the execution context"""
        ctx.addActor(self)
        self._context = ctx
        for child in self.children:
            child.setContext(ctx)

    def getContext(self):
        """Gets the execution context"""
        return self._context

    def tick(self):
        """Yields the event loop to another node"""
        raise UnsupportedOperation("Synchronous Actors do not support tick()")

    def sleep(self, seconds):
        """Makes the node sleep for the given seconds"""
        raise UnsupportedOperation("Synchronous Actors do not support sleep(seconds)")

    def handleException(self, exc):
        pass

    def stop(self):
        """Stop self and children"""
        self.handle(events.StopMessage)

    def start(self):
        """Starts the nodes"""
        if not self.running:
            self.handle(events.StartMessage)
            for child in self.children:
                child.start()
            super(Actor, self).start()

    def clear(self):
        """Removes all children from this node"""
        self._children = {}

    def __len__(self):
        return len(self._children)

    def add(self, node, lazy=False):
        """Adds a child to the current node."""
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
        if name not in self._children:
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
        if message:
            self.handle(message)
        self.running = False

    def execute(self):
        """Executes the preProcess, process, postProcess, scatter and gather methods"""
        self.running = True

        # self.log_info("Starting...")
        # while self.running:
        #     # self.log("Running...")
        #     try:
        #         message = self.inbox.get_nowait()
        #         self.handle(message)

        #         # yield to event loop after processing the message
        #         self.tick()

        #     except Empty:
        #         # Empty signifies that the queue is empty, so yield to another node
        #         self.tick()
        #         # pass

        # self.tick()
        # self.stop()
        # self.log_info("Exiting...")

    def _run(self):
        self.execute()

    def scatter(self, message, forward=False):
        """Sends the results of this node to its children."""
        self.emit('data', message, forward)

    def emit(self, channel, message, forward=False):
        """The `emit` function can be used to send 'out-of-band' messages to 
        any child nodes. This essentially allows a node to send special messages 
        to any nodes listening."""
        validateType('message', Message, message)
        message.channel = channel
        message.forward = forward
        # message.source = self
        self.log_trace("Sending message: %s on channel: %s" % (message, channel))
        for child in self.children:
            child.handle(message)
            # child.inbox.put_nowait(message)

        # yields to event loop
        # self.tick()

    def emitBatch(self, channel, messages, forward=False):
        """Add all messages to child queues before yielding"""
        for message in messages:
            message.channel = channel
            message.forward = forward
            # message.source = self
            self.log_trace("Sending message: %s on channel: %s" % (message, channel))
            for child in self.children:
                child.handle(message)
                # child.inbox.queue.extend(messages)
                # child.inbox.put_nowait(message)

        # for child in self.children:
            # child.inbox.queue.extend(messages)

        # yields to event loop
        # self.tick()

    def removeListener(self, channel, function):
        """Removes a listener function from a channel"""
        if channel in self.listeners:
            try:
                self.listeners[channel].remove(function)
                return True
            except ValueError:
                return False
        else:
            return False

    def on(self, channel, function):
        """Registers event listeners based on a channel.
        Note: All functions registered should have 2 arguments. The first is current node (ie. self) and the second is the data being transferred.
        """
        self.log_debug("Registering function on channel '%s'" % (channel))
        if channel in self.listeners:
            self.listeners[channel].append(function)
        else:
            self.listeners[channel] = [function]

    def handle(self, message):
        """Handles events received on a given channel and forwards it if allowed."""

        # logging message
        # if self.verbose:
        # self.log("Processing message: %s" % message)

        if message.channel in self.listeners:
            for function in self.listeners[message.channel]:
                # print function
                function(message)

        # forwards message if allowed
        if message.forward:
            self.log_trace("Forwarding message on channel: %s" % message.channel)
            for child in self.children:
                child.handle(message)
                # child.inbox.put_nowait(message)
            # self.tick()

    def insert(self, message):
        """Inserts a new message to be handled"""
        self.handle(message)

    def log(self, msg):
        """Logging capability is baked into every Node."""
        self.log_info(msg)

    def log_debug(self, msg):
        """Logs a DEBUG message"""
        log_debug(str(self.name).upper(), msg)

    def log_info(self, msg):
        """Logs an INFO message"""
        log_info(str(self.name).upper(), msg)

    def log_warning(self, msg):
        """Logs a WARNING message"""
        log_warning(str(self.name).upper(), msg)

    def log_error(self, msg):
        """Logs an ERROR message"""
        log_error(str(self.name).upper(), msg)

    def log_critical(self, msg):
        """Logs a CRITICAL message"""
        log_critical(str(self.name).upper(), msg)

    def log_exception(self, msg):
        """Logs an exception"""
        log_exception(str(self.name).upper(), msg)

    def log_trace(self, msg):
        """Logs low-level debug message"""
        log_trace(str(self.name).upper(), msg)

from . import events
