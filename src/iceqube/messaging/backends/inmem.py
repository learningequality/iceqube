import logging
from collections import defaultdict
from iceqube.common.six.moves.queue import Empty, Queue
from iceqube.messaging.backends.base import BaseMessagingBackend

logger = logging.getLogger(__name__)

INMEM_SUPER_MAILBOX = defaultdict(lambda: Queue())


class MessagingBackend(BaseMessagingBackend):
    def __init__(self, *args, **kwargs):
        pass

    def send(self, mailbox, message, *args, **kwargs):
        INMEM_SUPER_MAILBOX[mailbox]._put(message)
        logger.debug("SEND MAILBOX: {} MSG: {}".format(mailbox, message))

    def pop(self, mailbox, timeout=0.2):
        msg = INMEM_SUPER_MAILBOX[mailbox].get(block=True, timeout=timeout)
        logger.debug("POP MAILBOX: {} MSG: {}".format(mailbox, msg))
        return msg

    def popn(self, mailbox, n=-1):
        m = INMEM_SUPER_MAILBOX[mailbox]
        messages = []
        while 1:
            try:
                msg = m.get_nowait()
                messages.append(msg)
            except Empty:
                break
        return messages

    def count(self, mailbox):
        return INMEM_SUPER_MAILBOX[mailbox].qsize()

    def clear(self, mailbox):
        INMEM_SUPER_MAILBOX[mailbox].clear()

    def shutdown(self):
        pass
