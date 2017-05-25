import enum
import logging
from collections import namedtuple
from functools import partial

from barbequeue.common.utils import import_stringified_func, stringify_func

logger = logging.getLogger(__name__)


class Job(object):
    class State(enum.Enum):
        SCHEDULED = 0
        QUEUED = 1
        RUNNING = 2
        FAILED = 3
        CANCELED = 4
        COMPLETED = 5

    def __init__(self, func, *args, **kwargs):
        self.job_id = kwargs.pop('job_id', None)
        self.state = kwargs.pop('state', self.State.SCHEDULED)
        self.traceback = kwargs.pop('traceback', '')
        self.exception = kwargs.pop('exception', '')
        self.track_progress = kwargs.pop('track_progress', False)
        self.progress = 0
        self.total_progress = 0
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
        """
        return a function that executes the function assigned to this job.
        
        If job.track_progress is None (the default), the returned function accepts no argument
        and simply needs to be called. If job.track_progress is True, an update_progress function
        is passed in that can be used by the function to provide feedback progress back to the
        job scheduling system.
        
        :return: a function that executes the original function assigned to this job.
        """
        func = import_stringified_func(self.func)

        if self.track_progress:
            y = lambda p: func(update_progress=partial(p, self.job_id), *self.args, **self.kwargs)
        else:
            y = lambda: func(*self.args, **self.kwargs)

        return y

    @property
    def percentage_progress(self):
        """
        Returns a float between 0 and 1, representing the current job's progress in its task.
        
        :return: float corresponding to the total percentage progress of the job.
        """
        return float(self.progress) / self.total_progress

    def __repr__(self):
        return "<Job id: {id} state: {state} progress: {p}/{total} func: {func}>".format(id=self.job_id,
                                                                                         state=self.state.name,
                                                                                         func=self.func,
                                                                                         p=self.progress,
                                                                                         total=self.total_progress)

    def serialize(self):
        pass


class ProgressData(namedtuple("_ProgressData", ["id", "order", "data"])):
    pass


class Function(namedtuple("_Function", ["module", "funcname"])):
    def serialize(self):
        # Since this is all in memory, there is no need to serialize anything.
        raise NotImplementedError()
