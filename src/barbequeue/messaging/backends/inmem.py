import logging
import time
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from queue import Queue, Empty

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
    def wait(self, mailbox, timeout):
        """Wait until there's at least one item in the mailbox. 
        Raises Queue.Empty if the mailbox is still empty after the given timeout.
        
        Note: calling wait() and then pop() is not an atomic operation, 
        pop() may still fail if another object has popped in the meantime."""
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

    def wait(self, mailbox, timeout=None):
        m = INMEM_SUPER_MAILBOX[mailbox]

        timeout_increment = 0.1

        while True:
            if not m.empty():  # not empty, return now

                return True
            elif timeout <= 0:  # we've gone past our alloted timeout, so raise an error
                raise Empty
            else:
                time.sleep(timeout_increment)
                timeout -= timeout_increment

    def pop(self, mailbox, timeout=0.2):
        msg = INMEM_SUPER_MAILBOX[mailbox].get(block=True, timeout=timeout)
        logger.debug("POP MAILBOX: {} MSG: {}".format(mailbox, msg))
        return msg

    def count(self, mailbox):
        return INMEM_SUPER_MAILBOX[mailbox].qsize()

    def clear(self, mailbox):
        INMEM_SUPER_MAILBOX[mailbox].clear()
