import json
from six import string_types

# Default values
ADDRESS = '127.0.0.1'
PORT = 9876


class ConnectionClosed(Exception):
    pass


class ProtocolError(Exception):
    pass

# Message Types
CLEAR_QUEUE = "CLEAR_QUEUE"
GET_QUEUE = "GET_QUEUE"
SUBSCRIBE = "SUBSCRIBE"
UNSUBSCRIBE = "UNSUBSCRIBE"
SET_OPTIONS = "SET_OPTIONS"
RESPONSE = "RESPONSE"

# Responses
# Precanned responses that have special semantics
SERVER_EXIT = "SERVER_EXIT"


MESSAGE_SEPARATOR = "~"


def construct_message(queue, message):
    if not isinstance(queue, string_types):
        raise ValueError('Queue name must be a string, not %s' % queue)

    if len(queue) < 1:
        raise ValueError('Queue name must be at least one character in length')

    if MESSAGE_SEPARATOR in queue:
        raise ValueError("Queue name must not contain '{separator}'".format(separator=MESSAGE_SEPARATOR))

    message = json.dumps(message)

    if len(message) > 99999999:  # 100 MB max int that can fit in message header (8 characters, plus two controls)
        raise ValueError('Message cannot be 100MB or larger')

    return MESSAGE_SEPARATOR + str(len(message) + len(queue) + 1) + MESSAGE_SEPARATOR + queue + MESSAGE_SEPARATOR + message


def send_message(socket, queue, message):
    socket.send(construct_message(queue, message))


def get_message(socket, timeout=1):
    socket.settimeout(timeout)
    data = socket.recv(10).decode('utf-8')
    expected_length, data = validate_header(data)

    while len(data) < expected_length:
        data += socket.recv(expected_length - len(data)).decode('utf-8')

    if MESSAGE_SEPARATOR not in data:
        return None, data

    queue, message = data.split(MESSAGE_SEPARATOR, 1)

    return queue, json.loads(message)


def validate_header(data):
    """
    Validates that data is in the form of "~5~Hello", with ~ beginning messages, followed by the length of the
    message as an integer, followed by a ~, then the message.
    :param data: The raw data from the socket
    :return: (int, str) - the expected length of the message, the message
    """
    if not data:
        raise ConnectionClosed()

    if data[0] != MESSAGE_SEPARATOR:
        raise ProtocolError("Missing beginning {separator}".format(separator=MESSAGE_SEPARATOR))

    if MESSAGE_SEPARATOR not in data:
        raise ProtocolError("Missing {separator} after length".format(separator=MESSAGE_SEPARATOR))

    length, data = data[1:].split(MESSAGE_SEPARATOR, 1)

    try:
        length = int(length)
    except ValueError:
        raise ProtocolError("Length integer must be between '{separator}' and {separator}".format(separator=MESSAGE_SEPARATOR))

    return length, data
