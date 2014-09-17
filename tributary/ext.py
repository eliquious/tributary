import pkg_resources
import sys

__all__ = ['load_plugins']


def load_plugins():
    """Loads all the plugins which support tributary.ext as entry points."""
    for ep in pkg_resources.iter_entry_points('tributary.ext'):
        # print ep.__dict__
        plugin = ep.load()
        sys.modules['tributary.ext' + ep.name] = plugin
        globals()[ep.name.replace('.', '')] = plugin
        __all__.append(ep.name)

load_plugins()
