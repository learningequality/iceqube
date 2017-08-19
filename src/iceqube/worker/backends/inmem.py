import traceback

from concurrent.futures import CancelledError
from concurrent.futures._base import CANCELLED_AND_NOTIFIED, CANCELLED

from iceqube.exceptions import UserCancelledError
from iceqube.worker.backends.base import BaseWorkerBackend


class WorkerBackend(BaseWorkerBackend):
    # worker types
    PROCESS = 1
    THREAD = 0

    def __init__(self, worker_type=THREAD, *args, **kwargs):
        # Internally, we use conncurrent.future.Future to run and track
        # job executions. We need to keep track of which future maps to which
        # job they were made from, and we use the job_future_mapping dict to do
        # so.
        self.job_future_mapping = {}
        self.future_job_mapping = {}
        self.worker_type = worker_type
        super(WorkerBackend, self).__init__(*args, **kwargs)

        # transfer cancel notification mechanism to something that will work for multiprocess Windows and Android,
        # once we release kolibri 0.5.x
        self.cancel_notifications = {}

    def schedule_job(self, job):
        """
        schedule a job to the type of workers spawned by self.start_workers.


        :param job: the job to schedule for running.
        :return:
        """
        l = _reraise_with_traceback(job.get_lambda_to_execute())

        future = self.workers.submit(l, update_progress_func=self.update_progress, cancel_job_func=self._check_for_cancel)

        # assign the futures to a dict, mapping them to a job
        self.job_future_mapping[future] = job
        self.future_job_mapping[job.job_id] = future

        # callback for when the future is now!
        future.add_done_callback(self.handle_finished_future)
        # add the job to our cancel notifications data structure, with False at first
        self.cancel_notifications[job.job_id] = False

        return future

    def shutdown_workers(self, wait=True):
        self.workers.shutdown(wait=wait)

    def start_workers(self, num_workers):
        if self.worker_type == self.PROCESS:
            from concurrent.futures import ProcessPoolExecutor
            worker_executor = ProcessPoolExecutor
        elif self.worker_type == self.THREAD:
            from concurrent.futures import ThreadPoolExecutor
            worker_executor = ThreadPoolExecutor
        else:
            raise ValueError(
                "WorkerBackend.worker_type must be one of [WorkerBackend.PROCESS, WorkerBackend.THREAD]"
            )

        pool = worker_executor(max_workers=num_workers)
        return pool

    def handle_finished_future(self, future):
        # get back the job assigned to the future
        job = self.job_future_mapping[future]

        try:
            result = future.result()
        except CancelledError as e:
            last_stage = getattr(e, "last_stage", "")
            self.report_cancelled(job, last_stage=last_stage)
            return
        except Exception as e:
            self.report_error(job, e, e.traceback)
            return

        self.report_success(job, result)

    def cancel(self, job_id):
        """
        Request a cancellation from the futures executor pool.
        If that didn't work (because it's already running), then mark
        a special variable inside the future that we can check
        inside a special check_for_cancel function passed to the
        job.
        :param job_id:
        :return:
        """
        future = self.future_job_mapping[job_id]
        is_future_cancelled = future.cancel()

        if is_future_cancelled:  # success!
            return True
        else:
            if future.running():
                # Already running, but let's mark the future as cancelled
                # anyway, to make sure that calling future.result() will raise an error.
                # Our cancelling callback will then check this variable to see its state,
                # and exit if it's cancelled.
                from concurrent.futures._base import CANCELLED
                future._state = CANCELLED
                return False
            else:  # probably finished already, too late to cancel!
                return False

    def _check_for_cancel(self, job_id, current_stage=""):
        """
        Check if a job has been requested to be cancelled. When called, the calling function can
        optionally give the stage it is currently in, so the user has information on where the job
        was before it was cancelled.

        :param job_id: The job_id to check
        :param current_stage: Where the job currently is

        :return: raises a CancelledError if we find out that we were cancelled.
        """

        future = self.future_job_mapping[job_id]
        is_cancelled = future._state in [CANCELLED, CANCELLED_AND_NOTIFIED]

        if is_cancelled:
            raise UserCancelledError(last_stage=current_stage)


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
