import logging
import traceback
from abc import ABCMeta, abstractmethod

from iceqube.common.six.moves import queue

from iceqube.common.utils import InfiniteLoopThread
from iceqube.messaging.classes import (
    FailureMessage, MessageType, ProgressMessage, SuccessMessage,
    JobCanceledMessage, )

logger = logging.getLogger(__name__)


class BaseWorkerBackend(object):
    __metaclass__ = ABCMeta

    def __init__(self, incoming_message_mailbox, outgoing_message_mailbox, msgbackend, num_workers=3, *args, **kwargs):
        self.incoming_message_mailbox = incoming_message_mailbox
        self.outgoing_message_mailbox = outgoing_message_mailbox
        self.msgbackend = msgbackend

        self.workers = self.start_workers(num_workers=num_workers)
        self.message_processor = self.start_message_processing()

    @abstractmethod
    def schedule_job(self, job):
        """Manually schedule a job given by job."""
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
            job = msg.message['job']
            self.schedule_job(job)
        elif msg.type == MessageType.CANCEL_JOB:
            job_id = msg.message['job_id']
            self.cancel(job_id)

    def report_cancelled(self, job, last_stage):
        msg = JobCanceledMessage(job.job_id, is_successfully_canceled=True, last_stage=last_stage)
        self.msgbackend.send(self.outgoing_message_mailbox, msg)

    def report_success(self, job, result):
        msg = SuccessMessage(job.job_id, result)
        self.msgbackend.send(self.outgoing_message_mailbox, msg)

    def report_error(self, job, exc, trace):
        trace = traceback.format_exc()
        logger.warning("Job {} raised an exception: {}".format(job.job_id, trace))
        msg = FailureMessage(job.job_id, exc, trace)
        self.msgbackend.send(self.outgoing_message_mailbox, msg)

    def update_progress(self, job_id, progress, total_progress, stage=""):
        msg = ProgressMessage(job_id, progress, total_progress, stage)
        self.msgbackend.send(self.outgoing_message_mailbox, msg)