import logging
import time
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from iceqube.common.six.moves.queue import Empty, Queue

logger = logging.getLogger(__name__)

INMEM_SUPER_MAILBOX = defaultdict(lambda: Queue())


class BaseMessagingBackend(object):
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
    def pop(self, mailbox, timeout=None):
        pass

    @abstractmethod
    def popn(self, mailbox, n=-1):
        """
        Pop at most N number of messages from the mailbox without waiting. If n is less than 0, pop all messages
        it can.
        :type n: int
        :type mailbox: str
        :param mailbox: The mailbox on whose messages to pop from
        :param n: The number of messages to pop. If less than 0, pop all messages.
        :return: The list of messages
        """
        pass

    @abstractmethod
    def count(self, mailbox):
        pass

    @abstractmethod
    def clear(self, mailbox):
        pass


class MessagingBackend(BaseMessagingBackend):
    def __init__(self, *args, **kwargs):
        pass

    def send(self, mailbox, message, *args, **kwargs):
        INMEM_SUPER_MAILBOX[mailbox]._put(message)
        logger.debug("SEND MAILBOX: {} MSG: {}".format(mailbox, message))

    def wait(self, mailbox, timeout=None):
        m = INMEM_SUPER_MAILBOX[mailbox]

        timeout_increment = 0.1

        while True:
            if not m.empty():  # not empty, return now

                return True
            elif timeout <= 0:  # we've gone past our alloted timeout, so raise an error
                raise Empty("Queue currently empty.")
            else:
                time.sleep(timeout_increment)
                timeout -= timeout_increment

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
