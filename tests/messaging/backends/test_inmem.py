import random

import pytest
from barbequeue.messaging.backends import inmem
from barbequeue.messaging.classes import Message, MessageType


@pytest.fixture
def defaultbackend():
    b = inmem.MessagingBackend()
    yield b


@pytest.fixture
def otherbackend():
    b = inmem.MessagingBackend()
    yield b


@pytest.fixture
def msg():
    msg_types = [MessageType.JOB_FAILED, MessageType.JOB_STARTED, MessageType.JOB_UPDATED,
                 MessageType.JOB_COMPLETED, MessageType.START_JOB, MessageType.CANCEL_JOB]
    msgtype = random.choice(msg_types)
    m = Message(msgtype, "doesntmatter")
    yield m


class TestBackend:
    def test_can_send_and_read_to_the_same_mailbox(self, defaultbackend, msg):
        defaultbackend.send("pytesting", msg)

        newmsg = defaultbackend.pop("pytesting")

        assert newmsg.type == msg.type
        assert newmsg.message == msg.message
