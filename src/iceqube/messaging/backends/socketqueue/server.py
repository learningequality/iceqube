import collections
import signal
import socket
import uuid
from iceqube.compat import Event
from iceqube.compat import Thread
from iceqube.messaging.backends.socketqueue.common import ConnectionClosed
from iceqube.messaging.backends.socketqueue.common import get_message
from iceqube.messaging.backends.socketqueue.common import send_message
from iceqube.messaging.backends.socketqueue.common import ADDRESS
from iceqube.messaging.backends.socketqueue.common import PORT
from iceqube.messaging.backends.socketqueue.common import CLEAR_QUEUE
from iceqube.messaging.backends.socketqueue.common import GET_QUEUE
from iceqube.messaging.backends.socketqueue.common import SUBSCRIBE
from iceqube.messaging.backends.socketqueue.common import UNSUBSCRIBE
from iceqube.messaging.backends.socketqueue.common import RESPONSE
from iceqube.messaging.backends.socketqueue.common import SERVER_EXIT
from six.moves.socketserver import BaseRequestHandler, ThreadingMixIn, TCPServer


class ThreadedTCPServer(ThreadingMixIn, TCPServer, object):
    allow_reuse_address = True

    def __init__(self, server_address, RequestHandlerClass, exit_event, timeout):
        super(ThreadedTCPServer, self).__init__(server_address, RequestHandlerClass)
        self.exit_event = exit_event
        self.timeout = timeout
        self.queues = {}
        self.connections = {}


class TCPRequestHandler(BaseRequestHandler):

    def handle(self):
        conn_id = str(uuid.uuid4())
        self.server.connections[conn_id] = {
            'handler': self,
            'subscriptions': [conn_id]
        }
        self.respond(conn_id, True)

        while not self.server.exit_event.is_set():
            try:
                self.main_handle(conn_id, self.request)
            except (ConnectionClosed, socket.error):
                break

        try:
            self.respond(conn_id, SERVER_EXIT)
        except socket.error:
            pass

        if conn_id in self.server.connections:
            del self.server.connections[conn_id]

    def main_handle(self, conn_id, request):
        try:
            queue, message = get_message(request)

            if SUBSCRIBE in message:
                self.subscribe(conn_id, message[SUBSCRIBE])
                self.respond(conn_id, True)
            elif UNSUBSCRIBE in message:
                self.unsubscribe(conn_id, message[UNSUBSCRIBE])
                self.respond(conn_id, True)
            elif GET_QUEUE in message:
                self.get_queue(conn_id, message[GET_QUEUE])
            elif CLEAR_QUEUE in message:
                self.clear_queue(conn_id, message[CLEAR_QUEUE])
            else:
                self.respond(conn_id, True)
                self.broadcast(conn_id, queue, message)
                self.store_message(queue, message)

        except socket.timeout:
            pass
        except (ConnectionClosed, socket.error):
            raise
        except Exception as ex:
            self.respond(conn_id, str(ex))

    def respond(self, conn_id, text):
        send_message(self.request, conn_id, {RESPONSE: text})

    def subscribe(self, conn_id, queues):
        if not queues:
            return

        if conn_id not in self.server.connections:
            return

        if not isinstance(queues, (list, tuple)):
            queues = [queues]

        subs = self.server.connections[conn_id]['subscriptions']
        for q in queues:
            if q not in subs:
                subs.append(q)

    def unsubscribe(self, conn_id, queues):
        if not queues:
            return

        if conn_id not in self.server.connections:
            return

        if not isinstance(queues, (list, tuple)):
            queues = [queues]

        subs = self.server.connections[conn_id]['subscriptions']
        for q in queues:
            if q in subs:
                subs.remove(q)

    def broadcast(self, source, queue, message):
        for conn_id, d in self.server.connections.items():
            if conn_id == source and queue != conn_id:
                continue

            if queue in d['subscriptions']:
                send_message(d['handler'].request, queue, message)

    def store_message(self, queue, message):
        if queue not in self.server.queues:
            self.server.queues[queue] = collections.deque(maxlen=10)

        self.server.queues[queue].append(message)

    def get_queue(self, conn_id, queue):
        result = []
        if queue in self.server.queues:
            result = list(self.server.queues[queue])

        send_message(self.request, conn_id, result)

    def clear_queue(self, conn_id, queue):
        if queue in self.server.queues:
            self.server.queues[queue].clear()

        self.broadcast(conn_id, queue, {CLEAR_QUEUE: queue})


def _run(exit_event, ready_event, address, port):
    server = ThreadedTCPServer((address, port), TCPRequestHandler, exit_event, 1)
    while not server.exit_event.is_set():
        ready_event.set()
        server.handle_request()
    server.server_close()


class ServerThread(Thread):
    EXITING = None
    PROCESS = None

    @classmethod
    def start_command(cls, address=ADDRESS, port=PORT):
        cls.EXITING = Event()
        ready = Event()
        thread = cls(target=_run, args=(cls.EXITING, ready, address, port))
        cls.PROCESS = thread
        signal.signal(signal.SIGINT, cls.signal_handler)
        signal.signal(signal.SIGTERM, cls.signal_handler)
        thread.daemon = True
        thread.start()
        ready.wait()

    @classmethod
    def signal_handler(cls, signal, frame):
        cls.stop_command()

    @classmethod
    def stop_command(cls):
        cls.EXITING.set()
        cls.PROCESS.join()
