import traceback

from concurrent.futures import ThreadPoolExecutor

from barbequeue.worker.backends.base import BaseWorkerBackend


class WorkerBackend(BaseWorkerBackend):
    def __init__(self, *args, **kwargs):
        # Internally, we use conncurrent.future.Future to run and track
        # job executions. We need to keep track of which future maps to which
        # job they were made from, and we use the job_future_mapping dict to do
        # so.
        self.job_future_mapping = {}
        super(WorkerBackend, self).__init__(*args, **kwargs)

    def schedule_job(self, job):
        """
        schedule a job to the type of workers spawned by self.start_workers.
        
        
        :param job: the job to schedule for running.
        :return: 
        """
        l = _reraise_with_traceback(job.get_lambda_to_execute())

        if job.track_progress:
            future = self.workers.submit(l, self.update_progress)
        else:
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
        # get back the job assigned to the future
        job = self.job_future_mapping[future]

        try:
            result = future.result()
        except Exception as e:
            self.report_error(job, e, e.traceback)
            return

        self.report_success(job, result)


def _reraise_with_traceback(f):
    """
    Call the function normally. But if the function raises an error, attach the str(traceback)
    into the function.traceback attribute, then reraise the error.
    Args:
        f: The function to run.

    Returns: A function that wraps f, attaching the traceback if an error occurred.

    """

    def wrap(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            traceback_str = traceback.format_exc()
            e.traceback = traceback_str
            raise e

    return wrap
