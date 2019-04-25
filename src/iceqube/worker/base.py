from iceqube.common.compat import MULTIPROCESS
from iceqube.storage.backends import insqlite as storage_insqlite
from iceqube.worker.backends import inmem


MEMORY = storage_insqlite.StorageBackend.MEMORY


class Worker(object):
    # types of workers we can spawn
    PROCESS_BASED = inmem.WorkerBackend.PROCESS
    THREAD_BASED = inmem.WorkerBackend.THREAD

    def __init__(self, app, storage_path=MEMORY):
        if MULTIPROCESS:
            worker_type = self.PROCESS_BASED_BASED
        else:
            worker_type = self.THREAD_BASED
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
