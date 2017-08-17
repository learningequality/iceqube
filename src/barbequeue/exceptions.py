from concurrent.futures import CancelledError


class BaseError(Exception):
    """
    The base exception for all errors raised by barbequeue
    """
    pass

class TimeoutError(BaseError):
    """
    An error raised by barbequeue.storage.wait() when no job updates come in.
    """


class UserCancelledError(CancelledError):
    """
    An error raised when the user cancels the current job.
    """

    def __init__(self, last_stage):
        self.last_stage = last_stage
