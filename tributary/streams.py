#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This sub-module uses the Tributary module to process data streams. These stream
nodes handle each 'message' they are given 'completely'. By default, they do
not store any messages but instead pass them along to any child nodes.
"""

import tributary
from .core import Actor, BasePredicate, BaseOverride, Message
from .utilities import validateType
from .events import StartMessage, StopMessage, START, STOP
from gevent.queue import Empty

class LimitPredicate(BasePredicate):
    """LimitPredicate is used to limit the number of messages processed by the stream node"""
    def __init__(self, limit, offset=0):
        super(LimitPredicate, self).__init__()
        self.limit = limit + 1
        self.count = 0

    def apply(self, msg):
        self.count += 1
        if self.count < self.limit:
            return True
        return False

class SkipPredicate(BasePredicate):
    """SkipPredicate is used to skip a leading number of messages"""
    def __init__(self, skip):
        super(SkipPredicate, self).__init__()
        self.skip = skip + 1
        self.count = 0

    def apply(self, msg):
        self.count += 1
        if self.count > self.skip:
            return True
        return False

class SkipLimitPredicate(BasePredicate):
    """SkipLimitPredicate first skips a number of messages then limits the following messages."""
    def __init__(self, skip, limit):
        super(SkipLimitPredicate, self).__init__()
        self.skip = skip + 1
        self.limit = limit + 1
        self.count = 0

    def isValid(self, msg):
        self.count += 1
        if self.count > self.skip and self.count < self.limit + self.skip:
            return True
        return False

class StreamElement(Actor):
    """Streams send their processed data to their children as soon as they have
    finished processing it themselves."""
    def __init__(self, name):
        super(StreamElement, self).__init__(name)
        # set to True on the first message received
        self.initialized = False

        # The nodes' state are stored here. This variable should be set
        # each time a new message is received (in the process function). This
        # variable is what will be sent to the child nodes if it is not None.
        # Setting the value to None, is a way to clear the state and not send
        # it on to the child nodes.
        self.state = None

        # Stores all the filters and overrides for this node.
        self.modifiers = []

    def addFilter(self, _filter):
        """Adds a Filter to this stream. Filters must inherit from the BasePredicate class."""
        validateType("filter", BasePredicate, _filter)
        self.modifiers.append(_filter)
        return self

    def addOverride(self, override):
        """Adds an override to the stream. Overrides must inherit from the BaseOverride class."""
        validateType("arg", BaseOverride, override)
        self.modifiers.append(override)
        return self

    def limit(self, count, offset=0):
        """Limits the number of messages a stream can handle. An offset can be used to skip a number of messages"""
        if offset:
            self.addFilter(SkipLimitPredicate(offset, count))
        else:
            self.addFilter(LimitPredicate(count))
        return self

    def skip(self, count):
        """Skips a number of messages at the start of a stream."""
        self.addFilter(SkipPredicate(count))
        return self

    def execute(self):
        """Handles the data flow for streams"""
        self.running = True

        self.log("Starting...")
        while self.running:

            # Boolean flag to determine message validity
            valid = True
            
            try:
                message = self.inbox.get()

                # Iterates over all the filters and overrides to modify the
                # stream's default capability.
                for modifier in self.modifiers:
                    if isinstance(modifier, BaseOverride):
                        message = modifier.apply(message)
                    elif isinstance(modifier, BasePredicate):
                        if not modifier.apply(message):
                            valid = False

                            # Must be a break and not return because setting
                            # the initialization flag would be skipped if it
                            # needed to be set.
                            break

                # the incoming message was not filtered
                if valid:

                    # process the incoming message
                    self.handle(message)

                # yield to event loop after processing the message
                self.tick()

            except Empty:

                # Empty signifies that the queue is empty, so yield to another node
                self.tick()
            except Exception:
                tributary.log_exception(self.name, "Error in '%s': %s" % (self.__class__.__name__, self.name))
                self.tick()

        # self.tick()
        # self.stop()
        self.log("Exiting...")

class StreamProducer(StreamElement):
    """StreamProducer is an 'output' only stream. It does not process any
    incoming messages. Publishing messages is done manually inside the process method."""

    def validate(self, message):
        if message is not None:
            for modifier in self.modifiers:
                if isinstance(modifier, BaseOverride):
                    message = modifier.apply(message)
                elif isinstance(modifier, BasePredicate):
                    if not modifier.apply(message):
                        return False
        return True

    def execute(self):
        """Executes the preProcess, process, postProcess, scatter and gather methods"""

        # start
        self.log("Starting...")

        self.emit(START, StartMessage, forward=True)

        # process
        self.process(None)

        self.emit(STOP, StopMessage, forward=True)

        # done
        self.log("Exiting...")

        # stopping current node and sending stop message to child nodes
        # this is now handled by the engine
        # calling stop here prematurely cancels consumers. this could be bad if more than one producer it feeding this consumer.
        # self.stop()

    def emit(self, channel, message, forward=False):
        """The `emit` function can be used to send 'out-of-band' messages to 
        any child nodes. This essentially allows a node to send special messages 
        to any nodes listening."""
        validateType('message', Message, message)
        message.channel = channel
        message.forward = forward
        # message.source = self

        if self.validate(message):
            self.log_debug("Sending message: %s on channel: %s" % (message, channel))

            for child in self.children:
                child.insert(message)

        # yields to event loop
        self.tick()

# class Sink(StreamElement):
#     """Sink hold their state until the end then publishes it to any
#     child nodes. Children of Sink nodes should only expect one value."""
#     def execute(self, input=None):

#         # Should be false on the first message received.
#         # The preProcess function should handle any initialization for this stream.
#         if not self.initialized:
#             self.preProcess(input)
#             self.log("Initializing...")

#         # If the input is None, then it signals the end of the stream. None should
#         # be considered a poison-pill.
#         if input is not None:
#             try:
#                 # Iterates over all the filters and overrides to modify the
#                 # stream's default capability.
#                 for modifier in self.modifiers:
#                     if isinstance(modifier, BaseOverride):
#                         input = modifier.apply(input)
#                     elif isinstance(modifier, BasePredicate):
#                         if not modifier.apply(input):
#                             return

#                 # Processes the input. Stores any intermediate results in self.state.
#                 self.process(input)
#             except:
#                 tributary.log_exception(self.name, "Error in '%s': %s" % (self.__class__.__name__, self.name))
#         else:
#             # Closes the Sink node if None is received and the node has been initialized
#             if self.initialized:

#                 # Closes the Sink
#                 self.postProcess(None)

#                 # Published the state to any child nodes
#                 self.scatter(self.state)

#                 # Post processes any child nodes
#                 self.gather()
#                 self.log("Exiting...")

#         # Sets the init flag on the first message recieved
#         if not self.initialized:
#             self.initialized = True

# class StreamCounter(Sink):
#     """StreamCounter is a sink which only counts
#     the number of messages it has received."""
#     def __init__(self, name):
#         super(StreamCounter, self).__init__(name)
#         self.count = 0
#         self.state = Message()

#     def process(self, pkt):
#         self.count += 1

#     def postProcess(self, pkt):
#         self.state.name = self.name
#         self.state["count"] = self.count

