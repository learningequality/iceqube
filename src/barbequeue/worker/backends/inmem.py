import logging
import queue
import traceback
from threading import Event, Thread

from barbequeue import humanhash
from barbequeue.common.classes import BaseCloseableThread, Job
from barbequeue.messaging.backends.inmem import Backend as MsgBackend
from barbequeue.messaging.classes import MessageType, UnknownMessageError, Message


class Backend(object):
    def __init__(self, incoming_message_mailbox, outgoing_message_mailbox, num_threads=3, *args, **kwargs):
        self.incoming_message_mailbox = incoming_message_mailbox
        self.outgoing_message_mailbox = outgoing_message_mailbox
        self.jobqueue = queue.Queue(maxsize=num_threads)
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
            reportqueue=self.reportqueue,
            msgbackend=self.msgbackend,
            incoming_mailbox=self.incoming_message_mailbox,
            outgoing_mailbox=self.outgoing_message_mailbox,
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

    def shutdown(self, wait=True):
        self.monitor_shutdown_event.set()
        self.worker_shutdown_event.set()

        if wait:
            self.monitor_thread.join()

            for worker in self.worker_threads:
                worker.join()


class MonitorThread(BaseCloseableThread):
    def __init__(self, parent, jobqueue, reportqueue, msgbackend, incoming_mailbox, outgoing_mailbox, shutdownevent,
                 *args, **kwargs):
        self.parent = parent
        self.jobqueue = jobqueue
        self.reportqueue = reportqueue
        self.msgbackend = msgbackend
        self.incoming_mailbox = incoming_mailbox
        self.outgoing_mailbox = outgoing_mailbox

        super(MonitorThread, self).__init__(shutdown_event=shutdownevent, thread_name="MONITOR", *args, **kwargs)

    def main_loop(self, timeout):
        self.receive_incoming_messages(timeout)
        self.send_outgoing_messages(timeout)

    def send_outgoing_messages(self, timeout):
        # handle outgoing messages to send
        try:
            msg = self.reportqueue.get(block=True, timeout=timeout)
            self.msgbackend.send(self.outgoing_mailbox, msg)
        except queue.Empty:
            pass

    def receive_incoming_messages(self, timeout):
        try:
            msg = self.msgbackend.pop(self.incoming_mailbox, timeout=timeout)
            self.handle_incoming_message(msg)
        except queue.Empty:
            pass

    def handle_incoming_message(self, msg):
        if msg.type == MessageType.START_JOB:
            job = msg.message['job']
            self.start_job(job)
        elif msg.type == MessageType.CANCEL_JOB:
            raise NotImplemented()
        else:
            raise UnknownMessageError()

    def start_job(self, job):
        self.jobqueue.put(job)


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
            self.report_success(job, ret)
        except Exception as e:
            self.report_error(job, e)

    def report_success(self, job, return_value):
        self.logger.info("Job {} ran to completion.".format(job.job_id))
        self.jobqueue.task_done()

        # update job status
        job.state = Job.State.COMPLETED
        job.return_value = return_value

        msg = Message(type=MessageType.JOB_COMPLETED, message={'job': job, 'return_value': job.return_value})
        self.parent.reportqueue.put(msg)

    def report_error(self, job, e):
        self.logger.warning(
            "Got exception for job {}: {}".format(job.job_id, e))

        # update job status
        job.state = Job.State.FAILED
        job.traceback = traceback.format_exc()
        job.exception = str(e)

        msg = Message(type=MessageType.JOB_FAILED,
                      message={'job': job, 'traceback': job.traceback, 'exception': job.exception})
        self.parent.reportqueue.put(msg)

    @staticmethod
    def _generate_thread_id():
        id, hex = humanhash.uuid()
        return id
