try:
    # Import in order to check if multiprocessing is supported on this platform
    from multiprocessing import synchronization  # noqa
    # Proxy Process to Thread to allow seamless substitution
    from multiprocessing import Process as Thread
    from multiprocessing import *
except ImportError:
    from threading import *
