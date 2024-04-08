"""VAST Database Python SDK."""

from . import session

# A helper function, useful as a short-hand for Session c-tor: `session = vastdb.connect(...)`
connect = session.Session
connect.__name__ = 'connect'
