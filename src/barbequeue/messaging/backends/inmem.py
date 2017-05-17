import logging
from abc import ABCMeta, abstractmethod
from collections import defaultdict

from queue import Queue

logger = logging.getLogger(__name__)

INMEM_SUPER_MAILBOX = defaultdict(lambda: Queue())


class BaseBackend(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def send(self, mailbox, message, **kwargs):
        """
        Send a message to receivers of the mailbox.
        """
        pass

    @abstractmethod
    def pop(self, mailbox):
        pass

    @abstractmethod
    def count(self, mailbox):
        pass

    @abstractmethod
    def clear(self, mailbox):
        pass


class Backend(BaseBackend):
    def __init__(self, *args, **kwargs):
        pass

    def send(self, mailbox, message, *args, **kwargs):
        INMEM_SUPER_MAILBOX[mailbox].put(message)
        logger.debug("SEND MAILBOX: {} MSG: {}".format(mailbox, message))

    def pop(self, mailbox, timeout=0.2):
        msg = INMEM_SUPER_MAILBOX[mailbox].get(block=True, timeout=timeout)
        logger.debug("POP MAILBOX: {} MSG: {}".format(mailbox, msg))
        return msg

    def count(self, mailbox):
        return len(INMEM_SUPER_MAILBOX[mailbox])

    def clear(self, mailbox):
        INMEM_SUPER_MAILBOX[mailbox].clear()
