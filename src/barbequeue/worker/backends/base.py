from abc import abstractmethod, ABCMeta


class BaseBackend(object):
    __metaclass__ = ABCMeta

    def __init__(self, incoming_message_mailbox, outgoing_message_mailbox, msgbackend, num_workers=3, *args, **kwargs):
        self.incoming_message_mailbox = incoming_message_mailbox
        self.outgoing_message_mailbox = outgoing_message_mailbox
        self.msgbackend = msgbackend

        self.workers = self.start_workers(num_workers=num_workers)

    @abstractmethod
    def schedule_job(self, job):
        """Manually schedule a job given by job."""
        pass

    @abstractmethod
    def start_workers(self, num_workers):
        pass

    @abstractmethod
    def shutdown(self, wait):
        pass
