from iceqube.compat import MULTIPROCESS
from iceqube.storage.backends import insqlite as storage_insqlite
from iceqube.worker.backends import inmem


MEMORY = storage_insqlite.StorageBackend.MEMORY


class Engine(object):
    # types of workers we can spawn
    PROCESS_BASED = inmem.WorkerBackend.PROCESS
    THREAD_BASED = inmem.WorkerBackend.THREAD

    def __init__(self, app, worker_type=THREAD_BASED, storage_path=MEMORY):

        self._storage = storage_insqlite.StorageBackend(app, app, storage_path)
        self._workers = inmem.WorkerBackend(
            storage_backend=self._storage,
            worker_type=worker_type)

    def shutdown(self):
        """
        Shutdown the client and all of its managed resources:

        - the workers
        - the scheduler threads

        :return: None
        """
        self._storage.clear()
        self._workers.shutdown(wait=False)


class InMemEngine(Engine):
    """
    An engine that starts and runs all jobs in memory. In particular, the following iceqube components are all
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


class NoConfigEngine(Engine):
    """
    An engine that tries to run in multiprocess mode, with socket based messaging.
    If the environment is not hospitable to multiprocessing, it falls back to
    thread based workers with in memory messaging.
    """
    def __init__(self, app, *args, **kwargs):
        if MULTIPROCESS:
            super(NoConfigEngine, self).__init__(
                app,
                worker_type=self.PROCESS_BASED_BASED,
                *args,
                **kwargs)
        else:
            super(NoConfigEngine, self).__init__(
                app,
                worker_type=self.THREAD_BASED,
                *args,
                **kwargs)
