# Exceptions used in Tributary

__all__ = ["NotImplementedYet", "NodeDoesNotExist"]

class NotImplementedYet(Exception):
    """NotImplementedYet can be raised if a function has not been implemented. """
    def __init__(self, func_name):
        super(NotImplementedYet, self).__init__()
        self.func_name = func_name

    def __str__(self):
        return "Method not implemented yet: '%s'" % self.func_name

class NodeDoesNotExist(Exception):
    """NodeDoesNotExist is raised when a node is queried for
    which was never added to the parent node."""
    def __init__(self, source_name):
        super(NodeDoesNotExist, self).__init__()
        self.source_name = source_name
    
    def __str__(self):
        return "Node '%s' does not exist" % (self.source_name)
