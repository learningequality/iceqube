import time
from queue import Full, Empty
from threading import Event

from barbequeue.common.classes import BaseCloseableThread
from barbequeue.messaging.classes import MessageType, Message


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

        if msg.type == MessageType.JOB_UPDATED:
            pass
        elif msg.type == MessageType.JOB_COMPLETED:
            job = msg.message['job']
            self.storage_backend.complete_job(job.job_id)
        elif msg.type == MessageType.JOB_FAILED:
            pass
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
        except Full:
            self.logger.debug("Worker queue full; skipping scheduling of job {} for now.".format(next_job.job_id))
            return

    def _wait_until_messages_processed(self):
        """
        Dangerous! Only use this during tests. Only returns when all the messages in the incoming_message_mailbox
        drop to zero.
        :return: 
        """

        sleep_increment = 0.2

        while self.messaging_backend.count(self.incoming_message_mailbox) > 0:
            time.sleep(sleep_increment)
