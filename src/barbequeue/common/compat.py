"""
Everything in this module should have a Python 3 interface, but should be
compatible with Python 2
"""

import sys
import threading

# Because "Queue" was renamed to "queue" in Py3.
is_py2 = sys.version[0] == '2'
if is_py2:
    import Queue as queue  # noqa @UnusedImport
else:
    import queue as queue  # noqa @UnusedImport @Reimport

if is_py2:
    Event = threading._Event
else:
    Event = threading.Event
