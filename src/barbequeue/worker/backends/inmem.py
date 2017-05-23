from concurrent.futures import ThreadPoolExecutor

from barbequeue.worker.backends.base import BaseBackend


class Backend(BaseBackend):
    def start_reporter(self):
        pass

    def schedule_job(self, job):
        """Manually schedule a job given by job. A simple proxy to the Monitor
        Thread's self.start_job command."""
        l = job.get_lambda_to_execute()
        self.workers.submit(l)

    def shutdown(self, wait=True):
        self.workers.shutdown(wait=wait)

    def start_workers(self, num_workers):
        return ThreadPoolExecutor(max_workers=num_workers)


class Reporter(object):
    def __init__(self, reportqueue):
        pass
