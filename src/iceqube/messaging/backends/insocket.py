import logging
from iceqube.messaging.backends.socketqueue.common import ADDRESS
from iceqube.messaging.backends.socketqueue.common import PORT
from iceqube.messaging.backends.socketqueue.client import Queue
from iceqube.messaging.backends.socketqueue.server import ServerThread
from iceqube.messaging.classes import Message
from iceqube.common.six.moves.queue import Empty
from iceqube.messaging.backends.base import BaseMessagingBackend

logger = logging.getLogger(__name__)

INSOCKET_SUPER_MAILBOX = {}


class MessagingBackend(BaseMessagingBackend):
    def __init__(self, mailboxes=None, address=ADDRESS, port=PORT, start_server=False, *args, **kwargs):
        if start_server:
            ServerThread.start_command(address, port)
        if mailboxes is not None:
            for mailbox in mailboxes:
                INSOCKET_SUPER_MAILBOX[mailbox] = Queue(mailbox, address=address, port=port)

    def send(self, mailbox, message, *args, **kwargs):
        INSOCKET_SUPER_MAILBOX[mailbox].put(message)
        logger.debug("SEND MAILBOX: {} MSG: {}".format(mailbox, message))

    def pop(self, mailbox, timeout=0.2):
        msg = INSOCKET_SUPER_MAILBOX[mailbox].get(block=True, timeout=timeout)
        if type(msg) != Message:
            msg = Message(*msg)
        logger.debug("POP MAILBOX: {} MSG: {}".format(mailbox, msg))
        return msg

    def popn(self, mailbox, n=-1):
        m = INSOCKET_SUPER_MAILBOX[mailbox]
        messages = []
        while 1:
            try:
                msg = m.get_nowait()
                if type(msg) != Message:
                    msg = Message(*msg)
                messages.append(msg)
            except Empty:
                break
        return messages

    def count(self, mailbox):
        return INSOCKET_SUPER_MAILBOX[mailbox].qsize()

    def clear(self, mailbox):
        INSOCKET_SUPER_MAILBOX[mailbox].clear()

    def shutdown(self):
        for mailbox in INSOCKET_SUPER_MAILBOX.values():
            mailbox.shutdown()
