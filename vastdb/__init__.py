"""VAST Database Python SDK."""

from . import session

connect = session.Session
connect.__name__ = 'connect'
