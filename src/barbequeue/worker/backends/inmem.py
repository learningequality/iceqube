import logging
import queue
from threading import Event, Thread

from barbequeue import humanhash
from barbequeue.common.classes import BaseCloseableThread
from barbequeue.messaging.backends.inmem import Backend as MsgBackend
from barbequeue.messaging.classes import MessageType, UnknownMessageError, Message


class Backend(object):
    def __init__(self, mailbox, num_threads=3, *args, **kwargs):
        self.mailbox = mailbox
        self.jobqueue = queue.Queue()
        self.reportqueue = queue.Queue()
        self.msgbackend = MsgBackend()
        self.worker_threads = []

        # start the worker threads
        self.worker_shutdown_event = Event()
        self.start_worker_threads(num_threads)
        self.start_monitor_thread()

    def start_monitor_thread(self):
        # start the backend monitor thread to check our mailbox and handle
        # messages received from there
        self.monitor_shutdown_event = Event()
        self.monitor_thread = MonitorThread(
            parent=self,
            jobqueue=self.jobqueue,
            msgbackend=self.msgbackend,
            mailbox=self.mailbox,
            shutdownevent=self.monitor_shutdown_event)
        self.monitor_thread.start()

    def start_worker_threads(self, num_threads):
        for _ in range(num_threads):
            t = WorkerThread(
                parent=self,
                jobqueue=self.jobqueue,
                shutdownevent=self.worker_shutdown_event,
                reportqueue=self.reportqueue)
            self.worker_threads.append(t)
            # Label the worker threads as daemon, allowing the python
            # interpreter to shut down even if the worker threads are still
            # running
            # t.daemon = True
            t.start()

    def start_job(self, job):
        """Manually schedule a job given by job. A simple proxy to the Monitor
        Thread's self.start_job command."""
        self.monitor_thread.start_job(job)

    def shutdown(self):
        self.monitor_shutdown_event.set()
        self.worker_shutdown_event.set()


class MonitorThread(BaseCloseableThread):
    def __init__(self, parent, jobqueue, msgbackend, mailbox, shutdownevent,
                 *args, **kwargs):
        self.parent = parent
        self.jobqueue = jobqueue
        self.msgbackend = msgbackend
        self.mailbox = mailbox

        super(MonitorThread, self).__init__(shutdown_event=shutdownevent, thread_name="MONITOR", *args, **kwargs)

    def main_loop(self, timeout):
        try:
            msg = self.recv(timeout=timeout)
            self.handle_message(msg)
        except queue.Empty:
            pass

    def recv(self, timeout=0.2):
        return self.msgbackend.pop(self.mailbox, timeout=timeout)

    def handle_message(self, msg):
        if msg.type == MessageType.START_JOB:
            job = msg.message['job']
            self.start_job(job)
        elif msg.type == MessageType.CANCEL_JOB:
            raise NotImplemented()
        else:
            raise UnknownMessageError()

    def start_job(self, job):
        self.jobqueue.put(job)

    @staticmethod
    def _generate_thread_id():
        id, hex = humanhash.uuid()
        return id


class WorkerThread(Thread):
    def __init__(self, parent, jobqueue, reportqueue, shutdownevent, *args,
                 **kwargs):
        self.parent = parent
        self.jobqueue = jobqueue
        self.reportqueue = reportqueue
        self.shutdown_event = shutdownevent
        self.thread_id = self._generate_thread_id()
        self.logger = logging.getLogger(
            "{}.WORKER[{}]".format(__name__, self.thread_id))
        super(WorkerThread, self).__init__(*args, **kwargs)

    def run(self):
        self.logger.debug("Hello world! I'm a new worker thread.")
        while True:
            if self.shutdown_event.is_set():
                # start shutdown process
                self.logger.warning("WORKER shutdown event received; closing.")
                break
            else:
                # continue processing jobs
                self.process_jobs()

    def process_jobs(self):
        try:
            self.logger.debug("Getting next job in job queue")
            job = self.jobqueue.get(block=True, timeout=1)  # 1 second timeout
        except queue.Empty:
            self.logger.debug("No job found.")
            return

        func = job.get_lambda_to_execute()

        self.logger.info("Executing job {}".format(job.job_id))
        try:
            ret = func()
            self._notify_success(job)
        except Exception as e:
            self.alert_parent_of_error(job, e)
        finally:
            self.jobqueue.task_done()

    def alert_parent_of_error(self, job, e):
        self.logger.warning(
            "Got exception for job {}: {}".format(job.job_id, e))
        pass

    def _notify_success(self, job):
        msg = Message(type=MessageType.JOB_COMPLETED, message={'job': job})
        self.reportqueue.put(msg)

    @staticmethod
    def _generate_thread_id():
        id, hex = humanhash.uuid()
        return id
