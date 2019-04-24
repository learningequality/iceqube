import logging
import traceback
from abc import ABCMeta, abstractmethod

from iceqube.common.six.moves import queue

from iceqube.common.utils import InfiniteLoopThread
from iceqube.messaging.classes import MessageType

logger = logging.getLogger(__name__)


class BaseWorkerBackend(object):
    __metaclass__ = ABCMeta

    def __init__(self, incoming_message_mailbox, outgoing_message_mailbox, msgbackend, storage_backend, num_workers=3, *args, **kwargs):
        self.incoming_message_mailbox = incoming_message_mailbox
        self.outgoing_message_mailbox = outgoing_message_mailbox
        self.msgbackend = msgbackend
        self.storage_backend = storage_backend

        self.workers = self.start_workers(num_workers=num_workers)
        self.message_processor = self.start_message_processing()

    @abstractmethod
    def schedule_job(self, job_id):
        """Manually schedule a job given by job_id."""
        pass

    @abstractmethod
    def start_workers(self, num_workers):
        pass

    @abstractmethod
    def shutdown_workers(self, wait):
        pass

    @abstractmethod
    def cancel(self, job_id):
        pass

    def shutdown(self, wait=False):
        self.message_processor.stop()
        self.shutdown_workers(wait=wait)

    def start_message_processing(self):
        """
        Starts up the message processor thread, that continuously reads
        messages sent to self.incoming_message_mailbox, and starts or cancels jobs based on the message received.
        Returns: the Thread object.

        """
        t = InfiniteLoopThread(self.process_messages, thread_name="MESSAGEPROCESSOR", wait_between_runs=0.5)
        t.start()
        return t

    def process_messages(self):
        """
        Read from the incoming_message_mailbox and report to the storage backend
        based on the first message found there.
        Returns: None
        """
        try:
            msg = self.msgbackend.pop(self.incoming_message_mailbox)
            self.handle_incoming_message(msg)
        except queue.Empty:
            logger.debug("Worker message queue currently empty.")

    def handle_incoming_message(self, msg):
        """
        Start or cancel a job, based on the msg.

        If msg.type == MessageType.START_JOB, then start the job given by msg.job.

        If msg.type == MessageType.CANCEL_JOB, then try to cancel the job given by msg.job.job_id.

        Args:
            msg (barbequeue.messaging.classes.Message):

        Returns: None

        """
        if msg.type == MessageType.START_JOB:
            job_id = msg.job_id
            self.schedule_job(job_id)
        elif msg.type == MessageType.CANCEL_JOB:
            job_id = msg.job_id
            self.cancel(job_id)

    def report_cancelled(self, job, last_stage):
        self.storage_backend.mark_job_as_canceled(job.job_id)

    def report_success(self, job, result):
        self.storage_backend.complete_job(job.job_id)

    def report_error(self, job, exc, trace):
        trace = traceback.format_exc()
        logger.error("Job {} raised an exception: {}".format(job.job_id, trace))
        self.storage_backend.mark_job_as_failed(job.job_id, exc, trace)

    def update_progress(self, job_id, progress, total_progress, stage=""):
        self.storage_backend.update_job_progress(job_id, progress, total_progress)
