import importlib
import uuid

from barbequeue.common.classes import Job
from barbequeue.messaging.backends import inmem as messaging_inmem
from barbequeue.scheduler.classes import Scheduler
from barbequeue.storage.backends import inmem as storage_inmem
from barbequeue.worker.backends import inmem


class Client(object):
    def __init__(self, app, namespace, **config):
        self.storage_backend_module = config['storage_backend']
        self.storage = self.storage_backend_module.Backend(app, namespace)

    def schedule(self, func, *args, **kwargs):
        """
        Schedules a function func for execution.
        """

        updates_progress = kwargs.pop('updates_progress', False)

        # turn our function object into its fully qualified name if needed
        if callable(func):
            funcname = self.stringify_func(func)
        else:
            funcname = func

        job = Job(funcname, *args, **kwargs)
        job_id = self.storage.schedule_job(job)
        return job_id

    def cancel(self, job_id):
        """
        Mark a job as canceled and remove it from the list of jobs to be executed.
        Send a message to our workers to stop a job.
        """
        self.storage.cancel_job(job_id)

    def status(self, job_id):
        """
        Gets the status of a job given by job_id.
        """
        return self.storage.get_job(job_id)

    @staticmethod
    def stringify_func(func):
        assert callable(func), "function {} passed to stringify_func isn't a function!".format(func)

        fqn = "{module}.{funcname}".format(module=func.__module__, funcname=func.__name__)
        return fqn

    @staticmethod
    def import_stringified_func(funcstring):
        assert isinstance(funcstring, str)

        modulestring, funcname = funcstring.rsplit('.', 1)

        mod = importlib.import_module(modulestring)

        func = getattr(mod, funcname)
        return func


class InMemClient(Client):
    """
    A client that starts and runs all jobs in memory. In particular, the following barbequeue components are all running 
    their in-memory counterparts:
    
    - Scheduler
    - Job storage
    - Workers
    """

    def __init__(self, app, namespace, *args, **kwargs):
        # generate a unique name for our two mailboxes
        self.worker_mailbox_name = uuid.uuid4().hex
        self.scheduler_mailbox_name = uuid.uuid4().hex

        self._storage = storage_inmem.Backend(app, namespace)
        self._messaging = messaging_inmem.Backend()
        self._workers = inmem.Backend(incoming_message_mailbox=self.worker_mailbox_name)
        self._scheduler = Scheduler(self._storage, self._messaging, self._workers,
                                    worker_mailbox=self.worker_mailbox_name,
                                    incoming_mailbox=self.scheduler_mailbox_name)

        super(InMemClient, self).__init__(app, namespace, storage_backend=storage_inmem, *args, **kwargs)

    def shutdown(self):
        self._scheduler.shutdown()
        self._workers.shutdown()
