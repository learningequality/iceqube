import pytest
from iceqube.common.six.moves.queue import Empty
from iceqube.messaging.backends import inmem
from iceqube.messaging.classes import Message, MessageType

MAILBOX = "pytesting"


@pytest.fixture
def defaultbackend():
    b = inmem.MessagingBackend()
    yield b


@pytest.fixture(params=[MessageType.JOB_FAILED, MessageType.JOB_STARTED, MessageType.JOB_UPDATED,
                        MessageType.JOB_COMPLETED, MessageType.START_JOB, MessageType.CANCEL_JOB])
def msg(request):
    m = Message(request.param, "doesntmatter")
    yield m


class TestBackend:
    def test_can_send_and_read_to_the_same_mailbox(self, defaultbackend, msg):
        defaultbackend.send(MAILBOX, msg)

        newmsg = defaultbackend.pop(MAILBOX)

        assert newmsg.type == msg.type
        assert newmsg.message == msg.message

    def test_new_instance_can_send_and_read_to_the_same_mailbox(self, defaultbackend, msg):
        defaultbackend.send(MAILBOX, msg)

        otherbackend = inmem.MessagingBackend()

        newmsg = otherbackend.pop(MAILBOX)

        assert newmsg.type == msg.type
        assert newmsg.message == msg.message

    def test_pop_raise_empty_when_no_messages(self, defaultbackend):

        with pytest.raises(Empty):
            defaultbackend.pop(MAILBOX)

    def test_popn_return_empty_list_when_no_messages(self, defaultbackend):
        msgs = defaultbackend.popn(MAILBOX)

        assert len(msgs) == 0
        assert type(msgs) == list
