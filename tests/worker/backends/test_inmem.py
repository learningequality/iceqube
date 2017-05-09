import pytest
from barbequeue.common.classes import Job
from barbequeue.worker.backends import inmem


@pytest.fixture
def worker():
    b = inmem.Backend(mailbox="pytest")
    yield b
    b.shutdown()


@pytest.fixture
def msg():
    pass


ID_SET = None


def testfunc(val=None):
    global ID_SET
    ID_SET = val


@pytest.fixture
def job():
    global ID_SET
    test_func_name = "{module}.{func}".format(module=__name__, func="testfunc")
    yield Job(test_func_name, job_id="test", val="passme")
    ID_SET = None  #  reset the value set by testfunc


class TestBackend:
    def test_start_job_runs_a_function_defined_in_job(self, worker, job):

        reply = worker.start_job(job)
        # make sure tasks are processed before continuing
        worker.jobqueue.join()
        worker.shutdown()
        assert ID_SET
