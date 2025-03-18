"""VAST Database Python SDK."""

import functools

from . import session


# A helper function, useful as a short-hand for Session c-tor: `session = vastdb.connect(...)`
@functools.wraps(session.Session)
def connect(*args, **kwargs):  # noqa: D103
    return session.Session(*args, **kwargs)


def version():
    """Return VAST DB SDK version."""
    import importlib.metadata
    return importlib.metadata.distribution(__package__).version
