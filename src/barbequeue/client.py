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

        The only other special parameter is track_progress. If passed in and not None, the func will be passed in a
        keyword parameter called update_progress:

        def update_progress(progress, total_progress, stage=""):

        The running function can call the update_progress function to notify interested parties of the function's
        current progress.

        All other parameters are directly passed to the function when it starts running.

        :type func: callable or str
        :param func: A callable object that will be scheduled for running.
        :return: a string representing the job_id.
        """

        # if the func is already a job object, just schedule that directly.
        if isinstance(func, Job):
            job = func
        # else, turn it into a job first.
        else:
            job = Job(func, *args, **kwargs)

        job.track_progress = kwargs.pop('track_progress', False)
        job_id = self.storage.schedule_job(job)
        return job_id

    def cancel(self, job_id):
        """
        Mark a job as canceled and remove it from the list of jobs to be executed.
        Send a message to our workers to stop a job.

        :param job_id: the job_id of the Job to cancel.
        """
        self.storage.cancel_job(job_id)

    def status(self, job_id):
        """
        Returns a Job object corresponding to the job_id. From there, you can query for the following attributes:

        - function string to run
        - its current state (see Job.State for the list of states)
        - progress (returning an int), total_progress (returning an int), and percentage_progress
        (derived from running job.progress/total_progress)
        - the job.exception and job.traceback, if the job's function returned an error

        :param job_id: the job_id to get the Job object for
        :return: the Job object corresponding to the job_id
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
        """
        Shutdown the client and all of its managed resources:

        - the workers
        - the scheduler threads

        :return: None
        """
        self._scheduler.shutdown()
        self._workers.shutdown()
