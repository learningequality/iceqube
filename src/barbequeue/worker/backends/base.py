import traceback
from abc import ABCMeta, abstractmethod

from barbequeue.common.utils import InfiniteLoopThread
from barbequeue.messaging.classes import MessageType, ProgressMessage, SuccessMessage, FailureMessage


class BaseBackend(object):
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
    def shutdown(self, wait):
        pass

    def start_message_processing(self):
        t = InfiniteLoopThread(self.process_messages, thread_name="MESSAGEPROCESSOR", wait_between_runs=0.5)
        t.start()
        return t

    def process_messages(self):
        msg = self.msgbackend.pop(self.incoming_message_mailbox)
        self.handle_incoming_message(msg)

    def handle_incoming_message(self, msg):
        if msg.type == MessageType.START_JOB:
            job = msg.message['job']
            self.schedule_job(job)
        elif msg.type == MessageType.CANCEL_JOB:
            pass

    def report_success(self, job, result):
        msg = SuccessMessage(job.job_id, result)
        self.msgbackend.send(self.outgoing_message_mailbox, msg)

    def report_error(self, job, exc, trace):
        trace = traceback.format_exc()
        msg = FailureMessage(job.job_id, exc, trace)
        self.msgbackend.send(self.outgoing_message_mailbox, msg)

    def update_progress(self, job_id, progress, total_progress, stage=""):
        msg = ProgressMessage(job_id, progress, total_progress, stage)
        self.msgbackend.send(self.outgoing_message_mailbox, msg)
