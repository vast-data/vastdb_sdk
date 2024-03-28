from . import session

def connect(*args, **kw):
    return session.Session(*args, **kw)
