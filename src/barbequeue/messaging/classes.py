import enum
import json
from collections import namedtuple


class UnknownMessageError(Exception):
    pass


class MessageType(enum.Enum):
    # Job status messages
    JOB_FAILED = 0  # 0, so it can be falsey
    JOB_STARTED = 1
    JOB_UPDATED = 2
    JOB_COMPLETED = 3

    # Job command messages
    START_JOB = 101
    CANCEL_JOB = 102


class Message(namedtuple("_Message", ["type", "message"])):
    def serialize(self):
        # check that message type is in one of the message types we define
        assert self.type in (
            t.value for t in list(MessageType)
        ), "Message type not found in predetermined message type list!"

        return json.dumps({"type": self.type, "messsage": self.message})


class SuccessMessage(Message):
    def __new__(cls, job_id, result):
        msg = {'job_id': job_id, 'result': result}
        self = super(SuccessMessage, cls).__new__(cls, type=MessageType.JOB_COMPLETED, message=msg)
        return self
