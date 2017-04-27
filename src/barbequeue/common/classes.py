import enum
from collections import namedtuple


class Job(object):

    class State(enum.Enum):
        SCHEDULED = 0
        STARTED = 1
        RUNNING = 2
        FAILED = 3
        CANCELED = 4
        COMPLETED = 5

    def __init__(
        self, func_string, state=State.SCHEDULED, job_id=None, *args, **kwargs
    ):
        self.job_id = job_id
        self.state = state
        self.func = func_string
        self.args = args
        self.kwargs = kwargs

    def serialize(self):
        pass


class Client(object):

    def __init__(self, app, namespace, **config):
        self.backend_module = config['backend']
        self.backend = self.backend_module.Backend(app, namespace)

    def schedule(self, func, updates_progress=False, *args, **kwargs):
        """
        Schedules a function func for execution.
        """

        # turn our function object into its fully qualified name if needed
        if callable(func):
            funcname = self._stringify_func(func)
        else:
            funcname = func

        job = Job(funcname, args=args, kwargs=kwargs)
        job_id = self.backend.schedule_job(job)
        return job_id

    def cancel(self, job_id):
        """
        Mark a job as canceled and remove it from the list of jobs to be executed.
        Send a message to our workers to stop a job.
        """
        self.backend.cancel_job(job_id)

    def status(self, job_id):
        """
        Gets the status of a job given by job_id.
        """
        return self.backend.get_job(job_id)

    @staticmethod
    def _stringify_func(func):
        pass


class ProgressData(namedtuple("_ProgressData", ["id", "order", "data"])):
    pass


class Function(namedtuple("_Function", ["module", "funcname"])):

    def serialize(self):
        # Since this is all in memory, there is no need to serialize anything.
        raise NotImplementedError()
