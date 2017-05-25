from queue import Empty, Full
from threading import Event

from barbequeue.common.utils import BaseCloseableThread
from barbequeue.messaging.classes import Message, MessageType


class Scheduler(object):
    def __init__(self, storage_backend, messaging_backend, incoming_mailbox, worker_mailbox):
        self.incoming_mailbox = incoming_mailbox
        self.worker_mailbox = worker_mailbox

        self.storage_backend = storage_backend
        self.scheduler_shutdown_event = Event()

        self.messaging_backend = messaging_backend

        self.start_scheduler_thread()

    def start_scheduler_thread(self):
        self.scheduler_thread = SchedulerThread(messaging_backend=self.messaging_backend,
                                                storage_backend=self.storage_backend,
                                                incoming_message_mailbox=self.incoming_mailbox,
                                                worker_mailbox=self.worker_mailbox,
                                                shutdown_event=self.scheduler_shutdown_event, thread_name="SCHEDULER")
        self.scheduler_thread.setDaemon(True)
        self.scheduler_thread.start()

    def shutdown(self, wait=True):
        self.scheduler_shutdown_event.set()
        if wait:
            self.scheduler_thread.join()


class SchedulerThread(BaseCloseableThread):
    def __init__(self, storage_backend, messaging_backend, incoming_message_mailbox, worker_mailbox, *args, **kwargs):
        self.storage_backend = storage_backend
        self.worker_mailbox = worker_mailbox
        self.messaging_backend = messaging_backend
        self.incoming_message_mailbox = incoming_message_mailbox
        super(SchedulerThread, self).__init__(*args, **kwargs)

    def main_loop(self, timeout):
        self.schedule_next_job(timeout)
        self.handle_worker_messages(timeout)

    def handle_worker_messages(self, timeout):
        try:
            msg = self.messaging_backend.pop(self.incoming_message_mailbox, timeout=timeout)
        except Empty:
            self.logger.debug("No new messages from workers.")
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

    def schedule_next_job(self, timeout):
        next_job = self.storage_backend.get_next_scheduled_job()

        if not next_job:
            self.logger.debug("No job to schedule right now.")
            return

        try:
            self.messaging_backend.send(self.worker_mailbox,
                                        Message(type=MessageType.START_JOB, message={'job': next_job}))
            self.storage_backend.mark_job_as_queued(next_job.job_id)
        except Full:
            self.logger.debug("Worker queue full; skipping scheduling of job {} for now.".format(next_job.job_id))
            return
