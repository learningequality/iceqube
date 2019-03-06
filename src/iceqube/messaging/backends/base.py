from abc import ABCMeta, abstractmethod


class BaseMessagingBackend(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def send(self, mailbox, message, **kwargs):
        """
        Send a message to receivers of the mailbox.
        """
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

    @abstractmethod
    def shutdown(self):
        pass
