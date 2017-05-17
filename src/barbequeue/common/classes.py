import abc
import enum
import importlib
import logging
import threading
from collections import namedtuple

from barbequeue import humanhash

logger = logging.getLogger(__name__)


class Job(object):
    class State(enum.Enum):
        SCHEDULED = 0
        STARTED = 1
        RUNNING = 2
        FAILED = 3
        CANCELED = 4
        COMPLETED = 5

    def __init__(self, func_string, *args, **kwargs):
        self.job_id = kwargs.pop('job_id', None)
        self.state = kwargs.pop('state', self.State.SCHEDULED)
        self.func = func_string
        self.args = args
        self.kwargs = kwargs

    def get_lambda_to_execute(self):
        fqn = self.func
        modulename, funcname = fqn.rsplit('.', 1)
        mod = importlib.import_module(modulename)
        assert hasattr(
            mod, funcname), \
            "Module {} does not have attribute {}".format(
                mod, funcname)

        func = getattr(mod, funcname)

        y = lambda: func(*self.args, **self.kwargs)
        return y

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


class BaseCloseableThread(threading.Thread):
    """
    A base class for a thread that monitors an Event as a signal for shutting down, and a main_loop that otherwise loop
     until the shutdown event is received.
    """
    __metaclass__ = abc.ABCMeta

    DEFAULT_TIMEOUT_SECONDS = 0.2

    def __init__(self, shutdown_event, thread_name, *args, **kwargs):
        assert isinstance(shutdown_event, threading.Event)

        self.shutdown_event = shutdown_event
        self.thread_name = thread_name
        self.thread_id = self._generate_thread_id()
        self.logger = logging.getLogger("{module}.{name}[{id}]".format(module=__name__,
                                                                       name=self.thread_name,
                                                                       id=self.thread_id))
        super(BaseCloseableThread, self).__init__(*args, **kwargs)

    def run(self):
        self.logger.info("Started new {name} thread ID#{id}".format(name=self.thread_name,
                                                                    id=self.thread_id))

        while True:
            if self.shutdown_event.wait(self.DEFAULT_TIMEOUT_SECONDS):
                self.logger.warning("{name} shut down event received; closing.".format(name=self.thread_name))
                self.shutdown()
                break
            else:
                self.main_loop(timeout=self.DEFAULT_TIMEOUT_SECONDS)
                continue

    @abc.abstractmethod
    def main_loop(self, timeout):
        """
        The main loop of a thread. Run this loop if we haven't received any shutdown events in the last 
        timeout seconds. Normally this is used to read from a queue; you are encouraged to return from 
        this function if the timeout parameter has elapsed, to allow the thread to continue to check
        for the shutdown event.
        :param timeout: a parameter determining how long you can wait for a timeout.
        :return: None
        """
        pass

    @staticmethod
    def _generate_thread_id():
        uid, _ = humanhash.uuid()
        return uid

    def shutdown(self):
        # stub method, override if you need a more complex shut down procedure.
        pass
