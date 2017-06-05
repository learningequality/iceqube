import logging
from six.moves.queue import Empty, Full
from threading import Event

from barbequeue.common.utils import InfiniteLoopThread
from barbequeue.messaging.classes import Message, MessageType


class Scheduler(object):
    def __init__(self, storage_backend, messaging_backend, incoming_mailbox, worker_mailbox):
        self.incoming_mailbox = incoming_mailbox
        self.worker_mailbox = worker_mailbox

        self.storage_backend = storage_backend
        self.scheduler_shutdown_event = Event()

        self.messaging_backend = messaging_backend

        self.start_scheduler()
        self.start_worker_message_handler()

    def start_scheduler(self):
        self.scheduler_thread = InfiniteLoopThread(func=self.schedule_next_job, thread_name="SCHEDULER",
                                                   wait_between_runs=0.5)
        self.scheduler_thread.start()

    def start_worker_message_handler(self):
        self.worker_message_handler_thread = InfiniteLoopThread(func=lambda: self.handle_worker_messages(timeout=2),
                                                                thread_name="WORKERMESSAGEHANDLER",
                                                                wait_between_runs=0.5)
        self.worker_message_handler_thread.start()

    def shutdown(self, wait=True):
        self.scheduler_thread.stop()
        self.worker_message_handler_thread.stop()

        if wait:
            self.scheduler_thread.join()
            self.worker_message_handler_thread.join()

    def schedule_next_job(self):
        next_job = self.storage_backend.get_next_scheduled_job()
        # TODO: don't loop over if workers are already all running

        if not next_job:
            logging.debug("No job to schedule right now.")
            return

        try:
            self.messaging_backend.send(self.worker_mailbox,
                                        Message(type=MessageType.START_JOB, message={'job': next_job}))
            self.storage_backend.mark_job_as_queued(next_job.job_id)
        except Full:
            logging.debug("Worker queue full; skipping scheduling of job {} for now.".format(next_job.job_id))
            return

    def handle_worker_messages(self, timeout):
        try:
            msg = self.messaging_backend.pop(self.incoming_mailbox, timeout=timeout)
        except Empty:
            logging.debug("No new messages from workers.")
            return

        job_id = msg.message['job_id']
        actual_msg = msg.message

        if msg.type == MessageType.JOB_UPDATED:
            progress = actual_msg['progress']
            total_progress = actual_msg['total_progress']
            self.storage_backend.update_job_progress(job_id, progress, total_progress)
        elif msg.type == MessageType.JOB_COMPLETED:
            self.storage_backend.complete_job(job_id)
        elif msg.type == MessageType.JOB_FAILED:
            exc = actual_msg['exception']
            trace = actual_msg['traceback']
            self.storage_backend.mark_job_as_failed(job_id, exc, trace)
        else:
            self.logger.error("Unknown message type: {}".format(msg.type))
