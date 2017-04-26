# barbequeue
A self-contained pure python messaging and task queue system, with optional non-Python dependencies

# Installation

## Requirements


The base installation requires either Python 2.7, or 3.4+.

# Goals


1. Work on Windows 7 and above.
2. Work on Python 2.7 and 3.
3. Should the least amount of dependencies for the minimal use case. To use other backends and services (Django, Google PubSub etc.) will require more dependencies.


## Problems with current pure python task queues

1. Does not work with Windows out of the box.
2. Does not support progress tracking.

# Design

Barbequeue (or bbq for short) will be built as a layer on top of a messaging system. By allowing the messaging system to have multiple backends, from a
simple in-process and in-memory function, to a database backend, to a full blown global messaging service like Amazon SQS, bbq can scale up or change its features
based on the application's requirements.

Out of the box, bbq.messaging supports the following backends:


1. asyncio (lowest resources used, available on 3.4+)
1. socketserver (recommended for Windows machines, allows for multi-machine task delegation)

You can install additional bbq plugins to support the following backends:

1. Google Pub/Sub (TODO)
1. Amazon SQS (TODO)


for bbq.queue, there are the following task