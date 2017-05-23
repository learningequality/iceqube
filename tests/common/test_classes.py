import queue
from threading import Event

import pytest

from barbequeue.client import Client, InMemClient
from barbequeue.common.classes import Job
from barbequeue.common.utils import stringify_func, import_stringified_func
from barbequeue.storage.backends import inmem
from barbequeue.worker.backends import inmem as worker_inmem


@pytest.fixture
def backend():
    b = inmem.Backend(app="pytest", namespace="test")
    yield b
    b.clear()


# ARON: initialize the workers and the scheduler. We might need a shortcut function for that
@pytest.fixture
def inmem_worker_backend():
    w = worker_inmem.Backend()
    pass


@pytest.fixture
def client(backend):
    return Client("pytest", "test", storage_backend=inmem)


@pytest.fixture
def inmem_client():
    c = InMemClient('pytest', 'test')
    yield c
    c.shutdown()


@pytest.fixture
def simplejob():
    return Job("builtins.id")


@pytest.fixture
def scheduled_job(client, simplejob):
    job_id = client.schedule(simplejob)
    return client.storage.get_job(job_id)


@pytest.fixture
def flag():
    threading_flag = Event()
    yield threading_flag
    threading_flag.clear()


def set_flag(threading_flag):
    threading_flag.set()


class TestClient(object):
    def test_schedules_a_function(self, client, backend):
        job_id = client.schedule(id)

        # is the job recorded in the chosen backend?
        assert backend.get_job(job_id)

    def test_schedule_runs_function(self, inmem_client, flag):
        job_id = inmem_client.schedule(set_flag, flag)

        flag.wait(timeout=5)
        assert flag.is_set()

        # NOTE: Testing threaded code is not fun.
        # wait until we've received a new message in our inmem client
        #  messaging backend.
        try:
            inmem_client._messaging.wait(mailbox=inmem_client.scheduler_mailbox_name, timeout=2)
            inmem_client._messaging._wait_until_messages_processed()
        except queue.Empty:
            # Maybe it's been processed already... just continue anyway then.
            pass

        job = inmem_client.status(job_id)
        assert job.state == Job.State.COMPLETED

    def test_schedule_can_run_n_functions(self, inmem_client):
        n = 10
        events = [Event() for _ in range(n)]
        for e in events:
            inmem_client.schedule(set_flag, e)

        for e in events:
            assert e.wait(timeout=1)

    def test_stringify_func_is_importable(self, client):
        funcstring = stringify_func(set_flag)
        func = import_stringified_func(funcstring)

        assert set_flag == func

    def test_can_get_job_details(self, client, scheduled_job):
        assert client.status(
            scheduled_job.job_id
        ).job_id == scheduled_job.job_id

    def test_can_cancel_a_job(self, client, scheduled_job):
        client.cancel(scheduled_job.job_id)

        # Is our job marked as canceled?
        job = client.status(scheduled_job.job_id)
        assert job.state == Job.State.CANCELED
