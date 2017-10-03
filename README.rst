Iceqube
==========

A self-contained pure python messaging and task queue system, with
optional non-Python dependencies

Installation
============

Requirements
------------

The base installation requires either Python 2.7, or 3.4+.

Goals
=====

1. Work on Windows 7 and above.
2. Work on Python 2.7 and 3.
3. Should the least amount of dependencies for the minimal use case. To
   use other backends and services (Django, Google PubSub etc.) will
   require more dependencies.

Problems with current pure python task queues
---------------------------------------------

1. Does not work with Windows out of the box.
2. Does not support progress tracking.

Design
======
