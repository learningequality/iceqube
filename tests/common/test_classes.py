import pytest
from barbequeue.common.classes import Client
from barbequeue.storage.backends import inmem


@pytest.fixture
def client(backend):
    return Client(app="pytest", namespace="test", backend=inmem)


@pytest.fixture
def backend():
    b = inmem.Backend(app="pytest", namespace="test")
    yield b
    b.clear()


class TestClient(object):

    def test_schedules_a_function(self, client, backend):
        job_id = client.schedule(id)

        # is the job recorded in the chosen backend?
        assert backend.get_job(job_id)
