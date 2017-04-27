import pytest
from barbequeue.common.classes import Client, Job
from barbequeue.storage.backends import inmem


@pytest.fixture
def backend():
    b = inmem.Backend(app="pytest", namespace="test")
    yield b
    b.clear()


@pytest.fixture
def client(backend):
    return Client(app="pytest", namespace="test", backend=inmem)


@pytest.fixture
def simplejob():
    return Job("builtins.id")


@pytest.fixture
def scheduled_job(client, simplejob):
    job_id = client.schedule(simplejob)
    return client.backend.get_job(job_id)


class TestClient(object):

    def test_schedules_a_function(self, client, backend):
        job_id = client.schedule(id)

        # is the job recorded in the chosen backend?
        assert backend.get_job(job_id)

    def test_can_get_job_details(self, client, scheduled_job):
        assert client.status(
            scheduled_job.job_id
        ).job_id == scheduled_job.job_id

    def test_can_cancel_a_job(self, client, scheduled_job):
        client.cancel(scheduled_job.job_id)

        # Is our job marked as canceled?
        job = client.status(scheduled_job.job_id)
        assert job.state == Job.State.CANCELED
