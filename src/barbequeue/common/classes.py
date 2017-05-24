import enum
import logging
from collections import namedtuple

from barbequeue.common.utils import stringify_func, import_stringified_func

logger = logging.getLogger(__name__)


class Job(object):
    class State(enum.Enum):
        SCHEDULED = 0
        # STARTED = 1
        RUNNING = 2
        FAILED = 3
        CANCELED = 4
        COMPLETED = 5

    def __init__(self, func, *args, **kwargs):
        self.job_id = kwargs.pop('job_id', None)
        self.state = kwargs.pop('state', self.State.SCHEDULED)
        self.traceback = kwargs.pop('traceback', '')
        self.exception = kwargs.pop('exception', '')
        self.args = args
        self.kwargs = kwargs

        if callable(func):
            funcstring = stringify_func(func)
        elif isinstance(func, str):
            funcstring = func
        else:
            raise Exception(
                "Error in creating job. We do not know how to "
                "handle a function of type {}".format(type(func)))

        self.func = funcstring

    def get_lambda_to_execute(self):
        func = import_stringified_func(self.func)

        y = lambda: func(*self.args, **self.kwargs)
        return y

    def serialize(self):
        pass


class ProgressData(namedtuple("_ProgressData", ["id", "order", "data"])):
    pass


class Function(namedtuple("_Function", ["module", "funcname"])):
    def serialize(self):
        # Since this is all in memory, there is no need to serialize anything.
        raise NotImplementedError()


