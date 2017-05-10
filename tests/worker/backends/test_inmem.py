import pytest
from barbequeue.common.classes import Job
from barbequeue.messaging.classes import Message, MessageType
from barbequeue.worker.backends import inmem


@pytest.fixture
def mailbox():
    return "pytest"


@pytest.fixture
def worker(mailbox):
    b = inmem.Backend(mailbox=mailbox)
    yield b
    b.shutdown()


@pytest.fixture
def startmsg(job):
    msg = Message(type=MessageType.START_JOB, message={"job": job})
    return msg


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
        assert ID_SET


class TestMonitor:
    def test_handle_messages_start_message_starts_a_job(
            self, worker, startmsg, job, mocker):

        mocker.spy(worker.monitor_thread, 'start_job')
        worker.monitor_thread.handle_message(startmsg)

        start_job = worker.monitor_thread.start_job
        assert start_job.call_count == 1
        assert startmsg.message['job'] in start_job.call_args[0]

    def test_recv_reads_from_messaging_backend(self, worker, startmsg, mocker,
                                               mailbox):

        # we assume that monitor.recv mostly looks at the pop method.
        mocker.spy(worker.msgbackend, 'pop')
        worker.msgbackend.send(mailbox, startmsg)

        worker.monitor_thread.recv()

        assert worker.msgbackend.pop.call_count == 1
