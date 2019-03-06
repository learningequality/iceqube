import collections
import socket
from iceqube.common.six.moves.queue import Empty
from iceqube.messaging.backends.socketqueue.common import get_message
from iceqube.messaging.backends.socketqueue.common import send_message
from iceqube.messaging.backends.socketqueue.common import CLEAR_QUEUE
from iceqube.messaging.backends.socketqueue.common import GET_QUEUE
from iceqube.messaging.backends.socketqueue.common import SUBSCRIBE
from iceqube.messaging.backends.socketqueue.common import UNSUBSCRIBE
from iceqube.messaging.backends.socketqueue.common import RESPONSE
from iceqube.messaging.backends.socketqueue.common import SERVER_EXIT


class Queue(object):
    def __init__(self, namespace, address='127.0.0.1', port=9876):
        self._address = address
        self._port = port
        self._socket = None
        self._connection_id = None
        self._namespace = namespace
        self._queue = collections.deque(maxlen=10)
        queue = self._get_queue()
        self._queue.extend(queue)

    def _connect(self):
        if self._socket:
            return

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(30)
        self._socket.connect((self._address, self._port))
        self._connection_id, _ = get_message(self._socket)

        self._subscribe()

    def _close(self):
        if self._socket:
            self._socket.close()
            self._socket = None

    def _send_message(self, message):
        if not self._socket:
            self._connect()

        try:
            send_message(self._socket, self._namespace, message)
        except socket.error:
            # attempt to reconnect if there was a connection error
            self._close()
            self._connect()
            send_message(self._socket, self._namespace, message)

        try:
            return self._get_message()
        except socket.error:
            return None

    def _get_message(self, timeout=1):
        if not self._socket:
            self._connect()

        try:
            _, message = get_message(self._socket, timeout=timeout)
        except socket.timeout:
            return None
        except socket.error:
            # attempt to reconnect if there was a connection error
            self._close()
            self._connect()
            try:
                _, message = get_message(self._socket, timeout=timeout)
            except socket.timeout:
                return None

        if RESPONSE in message and message[RESPONSE] == SERVER_EXIT:
            self._close()

        if CLEAR_QUEUE in message:
            self._queue.clear()
            # Trying to clear the queue, do so, and then check for another
            # message
            return self._get_message(timeout=timeout)

        return message

    def _get_queue(self):
        return self._send_message({GET_QUEUE: self._namespace})

    def _subscribe(self):
        return self._send_message({SUBSCRIBE: self._namespace})

    def _unsubscribe(self):
        return self._send_message({UNSUBSCRIBE: self._namespace})

    def get(self, block=True, timeout=None):
        message = None
        if self._queue:
            message = self._queue.popleft()
        if block and message is None:
            message = self._get_message(timeout=timeout)
        if message is not None:
            return message
        raise Empty

    def get_nowait(self):
        return self.get(block=False)

    def put(self, message):
        self._queue.append(message)
        self._send_message(message)

    def clear(self):
        self._send_message({CLEAR_QUEUE: self._namespace})
        self._queue.clear()

    def qsize(self):
        self._get_message()
        return len(self._queue)

    def shutdown(self):
        self._close()
