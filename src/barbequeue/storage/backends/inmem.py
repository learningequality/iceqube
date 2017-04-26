import uuid
from collections import defaultdict, deque, namedtuple
from copy import copy

from barbequeue.common.classes import Job

INMEM_STORAGE = {}
INMEM_QUEUE = defaultdict(lambda: deque())


class Backend(object):

    def __init__(self, app, namespace, *args, **kwargs):
        self.app = app
        self.namespace = namespace
        self.namespace_id = uuid.uuid5(uuid.NAMESPACE_DNS, app + namespace).hex
        self.queue = INMEM_QUEUE[self.namespace_id]

    def schedule_job(self, job_details):
        """
        Add the job given by job_details to the job queue.

        Note: Does not actually run the job.
        """
        job_id = uuid.uuid4().hex
        job_details.job_id = job_id
        INMEM_STORAGE[job_id] = job_details

        # Add the job to the job queue
        self.queue.append(job_id)

        return job_id

    def cancel_job(self, job_id):
        """

        Mark the job as canceled. Does not actually try to cancel a running job.

        """
        job = self._get_job_nocopy(job_id)

        # Mark the job as canceled.
        job.state = Job.State.CANCELED

        # Remove it from the queue.
        self.queue.remove(job_id)

        return self.get_job(job)

    def _get_job_nocopy(self, job_id):
        job = INMEM_STORAGE.get(job_id)
        return job

    def get_next_job(self):
        return self.get_job(self.queue[0])

    def get_scheduled_jobs(self):
        return [self.get_job(id) for id in INMEM_QUEUE[self.namespace_id]]

    def get_job(self, job_id):
        job = self._get_job_nocopy(job_id)
        return copy(job)

    def clear(self):
        """
        Clear the queue and the job data.
        """
        scheduled_ids = list(self.queue)
        for j_id in scheduled_ids:
            INMEM_STORAGE.pop(j_id)
        self.queue.clear()

    def complete_job(self, job_id):
        job = self._get_job_nocopy(job_id)

        # mark the job as completed
        job.state = Job.State.COMPLETED

        # remove the job from the job queue.
        self.queue.remove(job_id)

        # TODO: add it to the list of completed jobs.


class Function(namedtuple("_Function", ["module", "funcname"])):

    def serialize(self):
        # Since this is all in memory, there is no need to serialize anything.
        return self
