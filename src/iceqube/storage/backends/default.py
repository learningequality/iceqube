import abc
from collections import defaultdict

from iceqube.compat import Event
from iceqube.exceptions import TimeoutError

JOB_EVENT_MAPPING = defaultdict(lambda: Event())


class BaseBackend(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def schedule_job(self, job_details):
        pass

    @abc.abstractmethod
    def mark_job_as_canceled(self, job_id):
        pass

    @abc.abstractmethod
    def get_next_scheduled_job(self):
        pass

    @abc.abstractmethod
    def get_scheduled_jobs(self):
        pass

    @abc.abstractmethod
    def get_job(self, job_id):
        pass

    @abc.abstractmethod
    def complete_job(self, job_id):
        pass

    @abc.abstractmethod
    def mark_job_as_running(self, job_id):
        pass

    @abc.abstractmethod
    def update_job_progress(self, job_id, progress):
        pass

    def wait_for_job_update(self, job_id, timeout=None):
        """
        Blocks until a job given by job_id has updated its state (canceled, completed, progress updated, etc.)
        if timeout is not None, then this function raises iceqube.exceptions.TimeoutError.

        :param job_id: the job's job_id to monitor for changes.
        :param timeout: if None, wait forever for a job update. If given, wait until timeout seconds, and then raise
        iceqube.exceptions.TimeoutError.
        :return: the Job object corresponding to job_id.
        """
        # internally, we register an Event object for each entry in this function.
        # when self.notify_of_job_update() is called, we call Event.set() on all events
        # registered for that job, thereby releasing any threads waiting for that specific job.

        event = JOB_EVENT_MAPPING[job_id]
        event.clear()
        result = event.wait(timeout=timeout)
        job = self.get_job(job_id)

        if result:
            return job
        else:
            raise TimeoutError("Job {} has not received any updates.".format(job_id))

    @staticmethod
    def notify_of_job_update(job_id):
        """
        Unblocks any thread waiting for job updates for the job given by job_id.

        :param job_id: any threads waiting for the job given by job_id is released from waiting.
        :return: None
        """
        JOB_EVENT_MAPPING[job_id].set()
