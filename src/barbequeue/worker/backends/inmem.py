from concurrent.futures import ThreadPoolExecutor

from barbequeue.messaging.classes import SuccessMessage
from barbequeue.worker.backends.base import BaseBackend


class Backend(BaseBackend):
    def __init__(self, *args, **kwargs):
        # Internally, we use conncurrent.future.Future to run and track
        # job executions. We need to keep track of which future maps to which
        # job they were made from, and we use the job_future_mapping dict to do
        # so.
        self.job_future_mapping = {}
        super(Backend, self).__init__(*args, **kwargs)

    def schedule_job(self, job):
        """Manually schedule a job given by job. A simple proxy to the Monitor
        Thread's self.start_job command."""
        l = job.get_lambda_to_execute()

        future = self.workers.submit(l)
        # assign the futures to a dict, mapping them to a job
        self.job_future_mapping[future] = job
        # callback for when the future is now!
        future.add_done_callback(self.handle_finished_future)

        return future

    def shutdown(self, wait=True):
        self.workers.shutdown(wait=wait)

    def start_workers(self, num_workers):
        return ThreadPoolExecutor(max_workers=num_workers)

    def handle_finished_future(self, future):
        job = self.job_future_mapping[future]

        try:
            result = future.result()
        except Exception as e:
            self.report_error(job, e)
            return

        self.report_success(job, result)

    def report_success(self, job, result):
        msg = SuccessMessage(job.job_id, result)
        self.msgbackend.send(self.outgoing_message_mailbox, msg)


class Reporter(object):
    def __init__(self, reportqueue):
        pass
