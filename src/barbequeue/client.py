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

    def schedule(self, funcstring, *args, **kwargs):
        """
        Schedules a function func for execution.
        """

        updates_progress = kwargs.pop('updates_progress', False)

        # if the funcstring is already a job object, just schedule that directly.
        if isinstance(funcstring, Job):
            job = funcstring
        # else, turn it into a job first.
        else:
            job = Job(funcstring, *args, **kwargs)

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
        self._workers = inmem.Backend(incoming_message_mailbox=self.worker_mailbox_name,
                                      outgoing_message_mailbox=self.scheduler_mailbox_name,
                                      msgbackend=self._messaging)
        self._scheduler = Scheduler(self._storage, self._messaging,
                                    worker_mailbox=self.worker_mailbox_name,
                                    incoming_mailbox=self.scheduler_mailbox_name)

        super(InMemClient, self).__init__(app, namespace, storage_backend=storage_inmem, *args, **kwargs)

    def shutdown(self):
        self._scheduler.shutdown()
        self._workers.shutdown()
