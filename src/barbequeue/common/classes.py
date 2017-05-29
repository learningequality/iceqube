import abc
import enum
import importlib
import logging
import threading
from collections import namedtuple

from barbequeue import humanhash
from barbequeue.common import compat

logger = logging.getLogger(__name__)


class Job(object):
    class State(enum.Enum):
        SCHEDULED = 0
        # STARTED = 1
        RUNNING = 2
        FAILED = 3
        CANCELED = 4
        COMPLETED = 5

    def __init__(self, func_string, *args, **kwargs):
        self.job_id = kwargs.pop('job_id', None)
        self.state = kwargs.pop('state', self.State.SCHEDULED)
        self.func = func_string
        self.traceback = kwargs.pop('traceback', '')
        self.exception = kwargs.pop('exception', '')
        self.args = args
        self.kwargs = kwargs

    def get_lambda_to_execute(self):
        if not isinstance(self.func, str):
            return self.func
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
        assert isinstance(shutdown_event, compat.Event)

        self.shutdown_event = shutdown_event
        self.thread_name = thread_name
        self.thread_id = self._generate_thread_id()
        self.logger = logging.getLogger("{module}.{name}[{id}]".format(module=__name__,
                                                                       name=self.thread_name,
                                                                       id=self.thread_id))
        self.full_thread_name = "{thread_name}-{thread_id}".format(thread_name=self.thread_name,
                                                                   thread_id=self.thread_id)
        super(BaseCloseableThread, self).__init__(name=self.full_thread_name, *args, **kwargs)

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
