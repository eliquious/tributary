# Exceptions used in Tributary

__all__ = ["NodeDoesNotExist"]

class NodeDoesNotExist(Exception):
    """NodeDoesNotExist is raised when a node is queried for
    which was never added to the parent node."""
    def __init__(self, source_name):
        super(NodeDoesNotExist, self).__init__()
        self.source_name = source_name
    
    def __str__(self):
        return "Node '%s' does not exist" % (self.source_name)
