import logging

from iceqube.common.six.moves.queue import Full
from iceqube.common.utils import InfiniteLoopThread
from iceqube.compat import Event
from iceqube.messaging.classes import CancelMessage
from iceqube.messaging.classes import Message
from iceqube.messaging.classes import MessageType


class Scheduler(object):
    def __init__(self, storage_backend, messaging_backend, incoming_mailbox,
                 worker_mailbox):
        self.incoming_mailbox = incoming_mailbox
        self.worker_mailbox = worker_mailbox

        self.storage_backend = storage_backend
        self.scheduler_shutdown_event = Event()

        self.messaging_backend = messaging_backend

        self.scheduler_thread = self.start_scheduler()
        self.worker_message_handler_thread = self.start_worker_message_handler()

    def start_scheduler(self):
        """
        Start the scheduler thread. This thread reads the queue of jobs to be
        scheduled and sends them to the workers.
        Returns: None

        """
        t = InfiniteLoopThread(
            func=self.schedule_next_job,
            thread_name="SCHEDULER",
            wait_between_runs=0.5)
        t.start()
        return t

    def start_worker_message_handler(self):
        """
        Start the worker message handler thread, that loops over messages from workers
        (job progress updates, failures and successes etc.) and then updates the job's status.
        Returns: None

        """
        t = InfiniteLoopThread(
            func=lambda: self.handle_worker_messages(timeout=2),
            thread_name="WORKERMESSAGEHANDLER",
            wait_between_runs=0.5)
        t.start()
        return t

    def shutdown(self, wait=True):
        """
        Shut down the worker message handler and scheduler threads.
        Args:
            wait: If true, block until both threads have successfully shut down. If False, return immediately.

        Returns: None

        """
        self.scheduler_thread.stop()
        self.worker_message_handler_thread.stop()

        if wait:
            self.scheduler_thread.join()
            self.worker_message_handler_thread.join()

    def request_job_cancel(self, job_id):
        """
        Send a message to the workers to cancel the job with job_id. We then mark the job in the storage
        as being canceled.

        :param job_id: the job to cancel
        :return: None
        """
        msg = CancelMessage(job_id)
        self.messaging_backend.send(self.worker_mailbox, msg)
        self.storage_backend.mark_job_as_canceling(job_id)

    def schedule_next_job(self):
        """
        Get the next job in the queue to be scheduled, and send a message
        to the workers to start the job.
        Returns: None

        """
        next_job = self.storage_backend.get_next_scheduled_job()
        # TODO: don't loop over if workers are already all running

        if not next_job:
            logging.debug("No job to schedule right now.")
            return

        try:
            self.messaging_backend.send(self.worker_mailbox,
                                        Message(
                                            type=MessageType.START_JOB,
                                            message={'job': next_job}))
            self.storage_backend.mark_job_as_queued(next_job.job_id)
        except Full:
            logging.debug(
                "Worker queue full; skipping scheduling of job {} for now.".format(next_job.job_id)
            )
            return

    def handle_worker_messages(self, timeout):
        """
        Read messages that are placed in self.incoming_mailbox,
        and then update the job states corresponding to each message.

        Args:
            timeout: How long to wait for an incoming message, if the mailbox is empty right now.

        Returns: None

        """
        msgs = self.messaging_backend.popn(self.incoming_mailbox, n=20)

        for msg in msgs:
            self.handle_single_message(msg)

    def handle_single_message(self, msg):
        """
        Handle one message and modify the job storage appropriately.
        :param msg: the message to handle
        :return: None
        """
        job_id = msg.message['job_id']
        actual_msg = msg.message
        if msg.type == MessageType.JOB_UPDATED:
            progress = actual_msg['progress']
            total_progress = actual_msg['total_progress']
            self.storage_backend.update_job_progress(job_id, progress,
                                                     total_progress)
        elif msg.type == MessageType.JOB_COMPLETED:
            self.storage_backend.complete_job(job_id)
        elif msg.type == MessageType.JOB_FAILED:
            exc = actual_msg['exception']
            trace = actual_msg['traceback']
            self.storage_backend.mark_job_as_failed(job_id, exc, trace)
        elif msg.type == MessageType.JOB_CANCELED:
            self.storage_backend.mark_job_as_canceled(job_id)
        else:
            self.logger.error("Unknown message type: {}".format(msg.type))
