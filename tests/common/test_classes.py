import tempfile
import time
import uuid
from threading import Event

import pytest

from barbequeue.client import SimpleClient
from barbequeue.common.classes import Job, State
from barbequeue.common.utils import import_stringified_func, stringify_func
from barbequeue.storage.backends import inmem


@pytest.fixture
def backend():
    with tempfile.NamedTemporaryFile() as f:
        b = inmem.StorageBackend(app="pytest", namespace="test", storage_path=f.name)
        yield b
        b.clear()


@pytest.fixture
def inmem_client():
    with tempfile.NamedTemporaryFile() as f:
        c = SimpleClient(app="pytest", storage_path=f.name)
        yield c
        c.shutdown()


@pytest.fixture
def simplejob():
    return Job("builtins.id")


@pytest.fixture
def scheduled_job(inmem_client, simplejob):
    job_id = inmem_client.schedule(simplejob)
    return inmem_client.storage.get_job(job_id)


def cancelable_job(is_running_event, is_not_canceled_event, check_for_cancel=None):
    """
    Test function for checking if a job is cancelable. Meant to be used in a job cancel
    test case.

    When first run, it calls the .set() function in the is_running_event passed in. This
    is meant to alert the test case that it started to run.

    It then calls the check_for_cancel, followed by a time.sleep function, 3 times. If it still
    continues (i.e. it did not recieve any cancel request), it then .set()s the is_not_canceled_event,
    alerting the test case that it waasn't canceled.

    :param is_running_event: An Event or EventProxy that the function sets when it starts running.
    :param is_not_canceled_event: An Event or EventProxy that the function sets when it runs to completion.
    :param check_for_cancel: A function that the BBQ framework passes in when a job is set to be cancellable.
    Calling this function makes the thread check if a cancellation has been requested, and then exits early if true.
    :return: None
    """

    is_running_event.set()  # mark the job as running

    for _ in range(3):
        time.sleep(0.5)
        check_for_cancel()

    is_not_canceled_event.set()


FLAG = False

EVENT_PROXY_MAPPINGS = {}


def _underlying_event(f):
    def func(self, *args, **kwargs):
        """
        Return the function f that's called with the EventProxy's
        matching Event, as the first argument.
        Returns:

        """
        event = EVENT_PROXY_MAPPINGS[self.event_id]
        return f(self, event, *args, **kwargs)

    return func


class EventProxy(object):
    """
    The tests in this file were originally written when we didn't need
    to pickle objects in storage. That way, we could use threading.Event
    objects to synchronize test and job function execution, and verify that
    things work across threads easily.

    With the move to ORMJob and pickling arguments, that means we can't
    pass in vanilla events anymore. The pickle module would either error out,
    or (with the dill extension to pickle), unpickle an event that's totally
    different from the previous event.

    To solve this, we use the EventProxy object. Whenever we instantiate this,
    we generate an id, and a corresponding event, and then store that event
    in a global dict with the id as the key. Calling in the EventProxy.wait, is_set
    or set methods makes us look up the event object based on the id stored
    in this event proxy instance, and then just call the appropriate method
    in that event class.

    Any extra args in the __init__ function is just passed to the event object
    creation.
    """

    def __init__(self, *args, **kwargs):
        self.event_id = uuid.uuid4().hex
        EVENT_PROXY_MAPPINGS[self.event_id] = Event(*args, **kwargs)

    @_underlying_event
    def wait(self, event, timeout=None):
        return event.wait(timeout=timeout)

    @_underlying_event
    def set(self, event):
        return event.set()

    @_underlying_event
    def is_set(self, event):
        return event.is_set()

    @_underlying_event
    def clear(self, event):
        return event.clear()


@pytest.fixture
def flag():
    e = EventProxy()
    yield e
    e.clear()


def set_flag(threading_flag):
    threading_flag.set()


def make_job_updates(flag, update_progress):
    for i in range(3):
        update_progress(i, 2)
    set_flag(flag)


def failing_func():
    raise Exception(
        "Test function failing_func has failed as it's supposed to.")


class TestClient(object):
    def test_schedules_a_function(self, inmem_client):
        job_id = inmem_client.schedule(id, 1)

        # is the job recorded in the chosen backend?
        assert inmem_client.status(job_id).job_id == job_id

    def test_schedule_runs_function(self, inmem_client, flag):
        job_id = inmem_client.schedule(set_flag, flag)

        flag.wait(timeout=5)
        assert flag.is_set()

        # sleep for half a second to make us switch to another thread
        time.sleep(0.5)
        try:
            inmem_client._storage.wait_for_job_update(job_id, timeout=2)
        except Exception:
            # welp, maybe a job update happened in between that schedule call and the wait call.
            # at least we waited!
            pass
        job = inmem_client.status(job_id)
        assert job.state == State.COMPLETED

    def test_schedule_can_run_n_functions(self, inmem_client):
        n = 10
        events = [EventProxy() for _ in range(n)]
        for e in events:
            inmem_client.schedule(set_flag, e)

        for e in events:
            assert e.wait(timeout=2)

    def test_scheduled_job_can_receive_job_updates(self, inmem_client, flag):
        job_id = inmem_client.schedule(
            make_job_updates, flag, track_progress=True)

        for i in range(2):
            inmem_client._storage.wait_for_job_update(job_id, timeout=2)
            job = inmem_client.status(job_id)
            assert job.state in [State.QUEUED, State.RUNNING, State.COMPLETED]

    def test_can_get_notified_of_job_failure(self, inmem_client):
        job_id = inmem_client.schedule(failing_func)

        job = inmem_client._storage.wait_for_job_update(job_id, timeout=2)
        assert job.state in [State.QUEUED, State.FAILED]

    def test_stringify_func_is_importable(self):
        funcstring = stringify_func(set_flag)
        func = import_stringified_func(funcstring)

        assert set_flag == func

    def test_can_get_job_details(self, inmem_client, scheduled_job):
        assert inmem_client.status(
            scheduled_job.job_id).job_id == scheduled_job.job_id

    def test_can_cancel_a_job(self, inmem_client):
        is_running_event = EventProxy()
        is_not_canceled_event = EventProxy()
        job_id = inmem_client.schedule(cancelable_job, is_running_event=is_running_event,
                                    is_not_canceled_event=is_not_canceled_event, cancellable=True)

        is_running_event.wait(1.0)
        # Job should be running after this point

        # Now let's cancel...
        inmem_client.cancel(job_id)
        # And check the job state to make sure it's marked as cancelling
        job = inmem_client.status(job_id)
        assert job.state == State.CANCELING

        # Let's wait for another job state change...
        job = inmem_client.wait_for_completion(job_id, timeout=2.0)
        # and hopefully it's canceled by this point
        assert job.state == State.CANCELED
