"""VAST Database Python SDK."""

import functools
import importlib.metadata

__version__ = importlib.metadata.distribution(__package__).version

from . import session


# A helper function, useful as a short-hand for Session c-tor: `session = vastdb.connect(...)`
@functools.wraps(session.Session)
def connect(*args, **kwargs):  # noqa: D103
    return session.Session(*args, **kwargs)
