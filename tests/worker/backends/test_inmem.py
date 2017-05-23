from threading import Event

import pytest

from barbequeue.common.classes import Job
from barbequeue.messaging.backends.inmem import Backend
from barbequeue.worker.backends import inmem


@pytest.fixture
def mailbox():
    return "pytest"

@pytest.fixture
def msgbackend():
    return Backend()


@pytest.fixture
def worker(mailbox, msgbackend):
    b = inmem.Backend(incoming_message_mailbox=mailbox, outgoing_message_mailbox=mailbox, msgbackend=msgbackend)
    yield b
    b.shutdown()

@pytest.fixture
def flag():
    e = Event()
    yield e
    e.clear()


def set_flag(threading_flag):
    threading_flag.set()


class TestWorker:
    def test_schedule_job_runs_job(self, worker, flag):
        job = Job(set_flag.__name__)
        worker.schedule_job(job)

        assert False
        assert flag.wait()
