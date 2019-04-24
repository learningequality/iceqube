from collections import namedtuple


class UnknownMessageError(Exception):
    pass


class MessageType(object):
    # Job command messages
    # Uses fixed length for predictable serialization length.
    START_JOB = "A"
    CANCEL_JOB = "B"


class Message(namedtuple("_Message", ["type", "job_id"])):
    def __init__(self, *args, **kwargs):
        super(Message, self).__init__()
        # check that message type is in one of the message types we define
        assert self.type in MessageType.__dict__.values(), "Message type not found in predetermined message type list!"


class StartMessage(Message):
    def __new__(cls, job_id):
        """
        Creates a Message that tells the worker(s) to cancel the job with job_id.

        :param job_id: The job_id of the job to cancel.
        :return: Message

        :type job_id: str
        """

        self = super(StartMessage, cls).__new__(cls, type=MessageType.START_JOB, job_id=job_id)
        return self


class CancelMessage(Message):
    def __new__(cls, job_id):
        """
        Creates a Message that tells the worker(s) to cancel the job with job_id.

        :param job_id: The job_id of the job to cancel.
        :return: Message

        :type job_id: str
        """

        self = super(CancelMessage, cls).__new__(cls, type=MessageType.CANCEL_JOB, job_id=job_id)
        return self
