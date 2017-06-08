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
    msgtype = random.choice(list(MessageType))
    m = Message(msgtype, "doesntmatter")
    yield m


class TestBackend:
    def test_can_send_and_read_to_the_same_mailbox(self, defaultbackend,
                                                   otherbackend, msg):
        defaultbackend.send("pytest", msg)

        newmsg = defaultbackend.pop("pytest")

        assert newmsg.type == msg.type
        assert newmsg.message == msg.message
