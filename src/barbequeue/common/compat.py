import sys

# Because "Queue" was renamed to "queue" in Py3.
is_py2 = sys.version[0] == '2'
if is_py2:
    import Queue as queue  # noqa @UnusedImport
else:
    import queue as queue  # noqa @UnusedImport @Reimport
