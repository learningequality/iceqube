import pytest
from iceqube.common.six.moves.queue import Empty
from iceqube.messaging.backends import insocket
from iceqube.messaging.backends.socketqueue.server import ServerThread
from iceqube.messaging.classes import Message, MessageType

MAILBOX = "pytesting"


@pytest.fixture(params=[MessageType.START_JOB, MessageType.CANCEL_JOB])
def msg(request):
    m = Message(request.param, "doesntmatter")
    yield m


@pytest.fixture
def server():
    ServerThread.start_command()
    yield True
    ServerThread.stop_command()


class TestBackend:

    def test_can_send_and_read_to_the_same_mailbox(self, msg, server):
        defaultbackend = insocket.MessagingBackend([MAILBOX])
        defaultbackend.send(MAILBOX, msg)

        newmsg = defaultbackend.pop(MAILBOX)

        assert newmsg.type == msg.type
        assert newmsg.job_id == msg.job_id

    def test_new_instance_can_send_and_read_to_the_same_mailbox(self, msg, server):
        defaultbackend = insocket.MessagingBackend([MAILBOX])
        defaultbackend.send(MAILBOX, msg)

        defaultbackend.shutdown()

        defaultbackend = insocket.MessagingBackend([MAILBOX])

        newmsg = defaultbackend.pop(MAILBOX)

        assert newmsg.type == msg.type
        assert newmsg.job_id == msg.job_id

    def test_pop_raise_empty_when_no_messages(self, server):
        defaultbackend = insocket.MessagingBackend([MAILBOX])

        with pytest.raises(Empty):
            defaultbackend.pop(MAILBOX)

    def test_popn_return_empty_list_when_no_messages(self, server):
        defaultbackend = insocket.MessagingBackend([MAILBOX])

        msgs = defaultbackend.popn(MAILBOX)

        assert len(msgs) == 0
        assert type(msgs) == list

    def test_count_return_zero_when_no_messages(self, server):
        defaultbackend = insocket.MessagingBackend([MAILBOX])

        assert defaultbackend.count(MAILBOX) == 0

    def test_count_return_one_when_one_message_sent(self, server):
        defaultbackend = insocket.MessagingBackend([MAILBOX])
        defaultbackend.send(MAILBOX, Message(MessageType.START_JOB, "doesntmatter"))

        assert defaultbackend.count(MAILBOX) == 1

    def test_count_return_one_when_one_message_sent_other_instance(self, server):
        defaultbackend = insocket.MessagingBackend([MAILBOX])
        otherbackend = insocket.MessagingBackend([MAILBOX])
        defaultbackend.send(MAILBOX, Message(MessageType.START_JOB, "doesntmatter"))

        assert otherbackend.count(MAILBOX) == 1

    def test_clear_empties_same_mailbox(self, server):
        defaultbackend = insocket.MessagingBackend([MAILBOX])
        defaultbackend.send(MAILBOX, Message(MessageType.START_JOB, "doesntmatter"))

        defaultbackend.clear(MAILBOX)

        assert defaultbackend.count(MAILBOX) == 0

    def test_clear_empties_other_mailbox(self, server):
        defaultbackend = insocket.MessagingBackend([MAILBOX])
        defaultbackend.send(MAILBOX, Message(MessageType.START_JOB, "doesntmatter"))

        defaultbackend.clear(MAILBOX)

        defaultbackend.shutdown()

        otherbackend = insocket.MessagingBackend([MAILBOX])

        assert otherbackend.count(MAILBOX) == 0
