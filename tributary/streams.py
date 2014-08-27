#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This sub-module uses the Tributary module to process data streams. These stream
nodes handle each 'message' they are given 'completely'. By default, they do
not store any messages but instead pass them along to any child nodes.
"""

import . as tributary
from .core import BaseNode, BasePredicate, BaseOverride, Message
from .utilities import validateType

class LimitPredicate(BasePredicate):
    """LimitPredicate is used to limit the number of messages processed by the stream node"""
    def __init__(self, limit, offset=0):
        super(LimitPredicate, self).__init__()
        self.limit = limit + 1
        self.count = 0

    def isValid(self, msg):
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

    def isValid(self, msg):
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

class StreamElement(BaseNode):
    """Streams send their processed data to their children as soon as they have
    finished processing it themselves."""
    def __init__(self, name, alwaysScatter=True):
        super(StreamElement, self).__init__(name)
        # set to True on the first message received
        self.initialized = False

        # Defaults to True. If True, this nodes' state are
        # published to the child nodes on every message
        # This can be set to False for certain types of nodes, such as Sinks.
        # Sinks never publish their state to their child nodes until finished.
        # This can also be set to False if the scattering is handled manually
        # inside the process function.
        self.alwaysScatter = alwaysScatter

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

    def execute(self, input=None):
        """Handles the data flow for streams."""

        # Should be false on the first message received.
        # The preProcess function should handle any initialization for this stream.
        if not self.initialized:
            self.preProcess(input)
            self.log("Initializing...")

        # If the input is None, then it signals the end of the stream. None should
        # be considered a poison-pill. Sending None to child nodes will signal
        # them to close as well.
        if input is not None:

            # Boolean flag to determine message validity
            valid = True
            try:
                # Iterates over all the filters and overrides to modify the
                # stream's default capability.
                for modifier in self.modifiers:
                    if isinstance(modifier, BaseOverride):
                        if modifier.isOverriden(input):
                            input = modifier.override(input)
                    elif isinstance(modifier, BasePredicate):
                        if not modifier.isValid(input):
                            valid = False

                            # Must be a break and not return because setting
                            # the initialization flag would be skipped if it
                            # needed to be set.
                            break

                # the incoming message was not filtered
                if valid:

                    # process the incoming message
                    # Must set the self.state variable.
                    self.process(input)

                    # send state to children if this flag is set and
                    #   self.state is not None
                    if self.alwaysScatter:
                        if self.state is not None:
                            self.scatter(self.state)
                            self.gather()
            except:
                tributary.log_exception(self.name, "Error in '%s': %s" % (self.__class__.__name__, self.name))
        else:

            # Closes the stream if the input is None and the stream has been
            # initialized.
            if self.initialized:
                # Finializes the stream node
                self.postProcess(None)

                # Kill child nodes
                self.scatter(None)

                # Post process the children
                self.gather()
                self.log("Exiting...")

        # Sets the init flag on the first time through
        if not self.initialized:
            self.initialized = True

    def scatter(self, input=None):
        """Sends the state of this node to its children."""
        for i, child in self.elements.itervalues():
            child.execute(input)

class StreamEngine(StreamElement):
    """StreamEngine starts the processing on all the streams."""
    def __init__(self, name="Engine"):
        super(StreamEngine, self).__init__(name)

    def process(self, input):
        """Doesn't do anything."""
        pass

    def start(self):
        """Executes the child streams"""
        try:
            self.scatter()
        except KeyboardInterrupt:
            self.log("Ctrl-C pressed: Exiting..")
        except:
            tributary.log_exception(self.name, "Error thrown:")

class StreamProducer(StreamElement):
    """StreamProducer is an 'output' only stream. It does not process any
    incoming messages. Publishing messages is done manually inside the process method."""

    def execute(self, _input=None):
        """Executes the preProcess, process, postProcess, scatter and gather methods"""

        # Initializes the stream
        self.preProcess(_input)

        # Publishes state to child nodes
        self.process(_input)

        # Closes the stream
        self.postProcess(_input)

        # Closes child nodes
        self.scatter(None)

        # Post-processes any child nodes
        self.gather()

    def scatter(self, input=None):
        """Sends the state of this node to its children."""
        if input is not None:
            for modifier in self.modifiers:
                if isinstance(modifier, BaseOverride):
                    input = modifier.apply(input)
                elif isinstance(modifier, BasePredicate):
                    if not modifier.apply(input):
                        return

        for i, child in self.elements.itervalues():
            child.execute(input)

class Sink(StreamElement):
    """Sink hold their state until the end then publishes it to any
    child nodes. Children of Sink nodes should only expect one value."""
    def execute(self, input=None):

        # Should be false on the first message received.
        # The preProcess function should handle any initialization for this stream.
        if not self.initialized:
            self.preProcess(input)
            self.log("Initializing...")

        # If the input is None, then it signals the end of the stream. None should
        # be considered a poison-pill.
        if input is not None:
            try:
                # Iterates over all the filters and overrides to modify the
                # stream's default capability.
                for modifier in self.modifiers:
                    if isinstance(modifier, BaseOverride):
                        input = modifier.apply(input)
                    elif isinstance(modifier, BasePredicate):
                        if not modifier.apply(input):
                            return

                # Processes the input. Stores any intermediate results in self.state.
                self.process(input)
            except:
                tributary.log_exception(self.name, "Error in '%s': %s" % (self.__class__.__name__, self.name))
        else:
            # Closes the Sink node if None is received and the node has been initialized
            if self.initialized:

                # Closes the Sink
                self.postProcess(None)

                # Published the state to any child nodes
                self.scatter(self.state)

                # Post processes any child nodes
                self.gather()
                self.log("Exiting...")

        # Sets the init flag on the first message recieved
        if not self.initialized:
            self.initialized = True

class StreamCounter(Sink):
    """StreamCounter is a sink which only counts
    the number of messages it has received."""
    def __init__(self, name):
        super(StreamCounter, self).__init__(name)
        self.count = 0
        self.state = Message()

    def process(self, pkt):
        self.count += 1

    def postProcess(self, pkt):
        self.state.name = self.name
        self.state["count"] = self.count

