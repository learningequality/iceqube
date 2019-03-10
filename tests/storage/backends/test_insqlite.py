import pytest

from iceqube.common.classes import Job, State
from iceqube.common.utils import stringify_func
from iceqube.storage.backends import insqlite


@pytest.fixture
def defaultbackend():
    backend = insqlite.StorageBackend(
        'pytest', 'pytest', storage_path=insqlite.StorageBackend.MEMORY)
    yield backend
    backend.clear()


@pytest.fixture
def simplejob():
    return Job(id)


class TestBackend:
    def test_can_schedule_single_job(self, defaultbackend, simplejob):
        job_id = defaultbackend.schedule_job(simplejob)

        new_job = defaultbackend.get_job(job_id)

        # Does the returned job record the function we set to run?
        assert str(new_job.func) == stringify_func(id)

        # Does the job have the right state (SCHEDULED)?
        assert new_job.state == State.SCHEDULED

        # Is the job part of the list of scheduled jobs?
        assert job_id in [
            j.job_id for j in defaultbackend.get_scheduled_jobs()
        ]

    def test_can_cancel_nonrunning_job(self, defaultbackend, simplejob):
        job_id = defaultbackend.schedule_job(simplejob)

        defaultbackend.mark_job_as_canceled(job_id)

        # is the job marked with the CANCELED state?
        assert defaultbackend.get_job(job_id).state == State.CANCELED

        # is the job not part of the list of scheduled jobs
        assert job_id not in [
            j.job_id for j in defaultbackend.get_scheduled_jobs()
        ]

    def test_can_get_first_job_queued(self, defaultbackend):
        job1 = Job(open)
        job2 = Job(open)

        job1_id = defaultbackend.schedule_job(job1)
        defaultbackend.schedule_job(job2)

        assert defaultbackend.get_next_scheduled_job().job_id == job1_id

    def test_can_complete_job(self, defaultbackend, simplejob):
        """
        When we call backend.complete_job, it should mark the job as finished, and
        remove it from the queue.
        """

        job_id = defaultbackend.schedule_job(simplejob)
        defaultbackend.complete_job(job_id)

        job = defaultbackend.get_job(job_id)

        # is the job marked as completed?
        assert job.state == State.COMPLETED
