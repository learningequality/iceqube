import logging
from abc import ABCMeta, abstractmethod
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

INMEM_SUPER_MAILBOX = defaultdict(lambda: deque())


class BaseBackend(metaclass=ABCMeta):

    @abstractmethod
    def send(self, mailbox, message, **kwargs):
        """
        Send a message to receivers of the mailbox.
        """
        pass

    @abstractmethod
    def peek(self, mailbox):
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

    def send(self, mailbox, message):
        INMEM_SUPER_MAILBOX[mailbox].append(message)
        logger.debug("SEND MAILBOX: {} MSG: {}".format(mailbox, message))

    def peek(self, mailbox):
        msg = INMEM_SUPER_MAILBOX[mailbox][0]
        logger.debug("PEEK MAILBOX: {} MSG: {}".format(mailbox, msg))
        return msg

    def pop(self, mailbox):
        msg = INMEM_SUPER_MAILBOX[mailbox].popleft()
        logger.debug("POP MAILBOX: {} MSG: {}".format(mailbox, msg))
        return msg

    def count(self, mailbox):
        return len(INMEM_SUPER_MAILBOX[mailbox])

    def clear(self, mailbox):
        INMEM_SUPER_MAILBOX[mailbox].clear()
