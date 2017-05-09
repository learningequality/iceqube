import importlib
import logging
import queue
import uuid
from threading import Event, Thread

from barbequeue import humanhash
from barbequeue.messaging.classes import MessageType

logging.basicConfig()


class Backend(object):
    def __init__(self, mailbox, num_threads=3, *args, **kwargs):
        self.mailbox = mailbox
        self.jobqueue = queue.Queue()
        self.reportqueue = queue.Queue()
        self.worker_threads = []
        self.worker_shutdown_event = Event()

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
            t.daemon = True
            t.start()

    def recv(self):
        """
        Read a message from the backend, and then pass it to the handle_msg function.
        """
        pass

    def handle_msg(self, msg):

        if msg.type == MessageType.START_JOB:
            pass

    def start_job(self, job):
        self.jobqueue.put(job)

    def shutdown(self):
        self.worker_shutdown_event.set()


class WorkerThread(Thread):
    def __init__(self, parent, jobqueue, reportqueue, shutdownevent, *args,
                 **kwargs):
        self.parent = parent
        self.jobqueue = jobqueue
        self.reportqueue = reportqueue
        self.shutdown_event = shutdownevent
        self.thread_id = self._generate_thread_id()
        self.logger = logging.getLogger(
            "{}.thread[{}]".format(__name__, self.thread_id))
        super(WorkerThread, self).__init__(*args, **kwargs)

    def run(self):
        self.logger.debug("Hello world! I'm a new worker thread.")
        while True:
            if self.shutdown_event.is_set():
                # start shutdown process
                self.logger.warning("Shutdown event received; closing.")
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
            self.notify_parent_of_success(job)
        except Exception as e:
            self.alert_parent_of_error(job, e)
        finally:
            self.jobqueue.task_done()

    def alert_parent_of_error(self, job, e):
        self.logger.warning(
            "Got exception for job {}: {}".format(job.job_id, e))
        pass

    def notify_parent_of_success(self, job):
        pass

    @staticmethod
    def _generate_thread_id():
        id, hex = humanhash.uuid()
        return id
