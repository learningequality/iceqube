from iceqube.common import SCHEDULER_MAILBOX
from iceqube.common import WORKER_MAILBOX
from iceqube.messaging.backends import inmem as messaging_inmem
from iceqube.scheduler.classes import Scheduler
from iceqube.storage.backends import inmem as storage_inmem
from iceqube.worker.backends import inmem


MEMORY = storage_inmem.StorageBackend.MEMORY
MEMORY_MESSAGING = messaging_inmem


class Engine(object):
    # types of workers we can spawn
    PROCESS_BASED = inmem.WorkerBackend.PROCESS
    THREAD_BASED = inmem.WorkerBackend.THREAD

    def __init__(self, app, worker_type=THREAD_BASED, storage_path=MEMORY, messaging=MEMORY_MESSAGING):

        self.worker_mailbox_name = WORKER_MAILBOX.format(app=app)
        self.scheduler_mailbox_name = SCHEDULER_MAILBOX.format(app=app)
        self._storage = storage_inmem.StorageBackend(app, app, storage_path)
        self._messaging = messaging.MessagingBackend(mailboxes=[self.worker_mailbox_name, self.scheduler_mailbox_name], start_server=True)
        self._workers = inmem.WorkerBackend(
            incoming_message_mailbox=self.worker_mailbox_name,
            outgoing_message_mailbox=self.scheduler_mailbox_name,
            msgbackend=self._messaging,
            worker_type=worker_type)
        self._scheduler = Scheduler(
            self._storage,
            self._messaging,
            worker_mailbox=self.worker_mailbox_name,
            incoming_mailbox=self.scheduler_mailbox_name)

    def shutdown(self):
        """
        Shutdown the client and all of its managed resources:

        - the workers
        - the scheduler threads

        :return: None
        """
        self._storage.clear()
        self._scheduler.shutdown(wait=False)
        self._workers.shutdown(wait=False)
        self._messaging.shutdown()


class InMemEngine(Engine):
    """
    A client that starts and runs all jobs in memory. In particular, the following iceqube components are all
    running
    their in-memory counterparts:

    - Scheduler
    - Job storage
    - Workers
    """

    def __init__(self, app, *args, **kwargs):
        super(InMemEngine, self).__init__(
            app,
            worker_type=self.THREAD_BASED,
            storage_path=self.MEMORY,
            *args,
            **kwargs)
