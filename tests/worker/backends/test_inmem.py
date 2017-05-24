from threading import Event

import pytest

from barbequeue.common.classes import Job
from barbequeue.messaging.backends.inmem import Backend
from barbequeue.messaging.classes import MessageType
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


def set_flag(threading_flag):
    threading_flag.set()


class TestWorker:
    def test_schedule_job_runs_job(self, worker):
        flag = Event()
        job = Job(set_flag, flag)
        worker.schedule_job(job)

        assert flag.wait(timeout=2)

    def test_schedule_job_sends_message_on_success(self, worker, mocker):
        mocker.spy(worker.msgbackend, 'send')

        # this job should never fail.
        job = Job(id, 9)
        future = worker.schedule_job(job)
        # wait for the result to finish
        future.result()

        # verify that we sent a message through our backend
        assert worker.msgbackend.send.call_count == 1

        # verify that we passed in a success message,
        # that includes the job_id, and the result
        call_args = worker.msgbackend.send.call_args
        message = call_args[0][1]
        assert message.type == MessageType.JOB_COMPLETED
        assert message.message['result'] == future.result()
        # verify that we're sending the job_id in the message
        assert message.message['job_id'] == job.job_id
