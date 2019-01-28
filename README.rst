Iceqube
==========

.. image:: https://badge.fury.io/py/iceqube.svg
   :target: https://pypi.python.org/pypi/iceqube/
.. image:: https://travis-ci.com/learningequality/iceqube.svg
  :target: https://travis-ci.com/learningequality/iceqube
.. image:: http://codecov.io/github/learningequality/iceqube/coverage.svg?branch=master
  :target: http://codecov.io/github/learningequality/iceqube?branch=master
.. image:: https://readthedocs.org/projects/iceqube/badge/?version=latest
  :target: http://iceqube.readthedocs.org/en/latest/
.. image:: https://img.shields.io/badge/irc-%23kolibri%20on%20freenode-blue.svg
  :target: http://webchat.freenode.net?channels=%23kolibri

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
