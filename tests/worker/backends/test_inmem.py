import pytest
import time

from iceqube.common.classes import Job
from iceqube.common.classes import State
from iceqube.storage.backends.insqlite import StorageBackend
from iceqube.worker.backends import inmem


@pytest.fixture
def storage_backend():
    return StorageBackend("test", "test", StorageBackend.MEMORY)


@pytest.fixture
def worker(storage_backend):
    b = inmem.WorkerBackend(storage_backend=storage_backend)
    yield b
    b.shutdown()


class TestWorker:
    def test_enqueue_job_runs_job(self, worker):
        job = Job(id, 9)
        worker.storage_backend.enqueue_job(job)

        while job.state == State.QUEUED:
            job = worker.storage_backend.get_job(job.job_id)
            time.sleep(0.5)
        try:
            # Get the future, or pass if it has already been cleaned up.
            future = worker.future_job_mapping[job.job_id]

            future.result()
        except KeyError:
            pass

        assert job.state == State.COMPLETED

    def test_enqueue_job_writes_to_storage_on_success(self, worker, mocker):
        mocker.spy(worker.storage_backend, 'complete_job')

        # this job should never fail.
        job = Job(id, 9)
        worker.storage_backend.enqueue_job(job)

        while job.state == State.QUEUED:
            job = worker.storage_backend.get_job(job.job_id)
            time.sleep(0.5)

        try:
            # Get the future, or pass if it has already been cleaned up.
            future = worker.future_job_mapping[job.job_id]

            future.result()
        except KeyError:
            pass

        # verify that we sent a message through our backend
        assert worker.storage_backend.complete_job.call_count == 1

        call_args = worker.storage_backend.complete_job.call_args
        job_id = call_args[0][0]
        # verify that we're setting the correct job_id
        assert job_id == job.job_id
