import pytest
import uuid

from iceqube.common.classes import Job
from iceqube.common.classes import State
from iceqube.messaging.backends.inmem import MessagingBackend
from iceqube.storage.backends.insqlite import StorageBackend
from iceqube.worker.backends import inmem


@pytest.fixture
def mailbox():
    return uuid.uuid4().hex


@pytest.fixture
def msgbackend():
    return MessagingBackend()


@pytest.fixture
def storage_backend():
    return StorageBackend("test", "test", StorageBackend.MEMORY)


@pytest.fixture
def worker(mailbox, msgbackend, storage_backend):
    b = inmem.WorkerBackend(incoming_message_mailbox=mailbox, outgoing_message_mailbox=mailbox, msgbackend=msgbackend, storage_backend=storage_backend)
    yield b
    b.shutdown()


class TestWorker:
    def test_schedule_job_runs_job(self, worker):
        job = Job(id, 9)
        worker.storage_backend.schedule_job(job)
        future = worker.schedule_job(job.job_id)

        job = worker.storage_backend.get_job(job.job_id)

        future.result()

        assert job.state == State.COMPLETED

    def test_schedule_job_writes_to_storage_on_success(self, worker, mocker):
        mocker.spy(worker.storage_backend, 'complete_job')

        # this job should never fail.
        job = Job(id, 9)
        worker.storage_backend.schedule_job(job)
        future = worker.schedule_job(job.job_id)
        # wait for the result to finish
        future.result()

        # verify that we sent a message through our backend
        assert worker.storage_backend.complete_job.call_count == 1

        call_args = worker.storage_backend.complete_job.call_args
        job_id = call_args[0][0]
        # verify that we're setting the correct job_id
        assert job_id == job.job_id
