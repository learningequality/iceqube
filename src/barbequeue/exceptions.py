class BaseError(Exception):
    """
    The base exception for all errors raised by barbequeue
    """
    pass

class TimeoutError(BaseError):
    """
    An error raised by barbequeue.storage.wait() when no job updates come in.
    """
