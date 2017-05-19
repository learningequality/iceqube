import abc


class BaseBackend(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def schedule_job(self, job_details):
        pass

    @abc.abstractmethod
    def cancel_job(self, job_id):
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
