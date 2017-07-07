import time
import threading

from barbequeue.common.utils import InfiniteLoopThread


class TestBaseCloseableThread(object):
    def test_handles_interpreter_shutting_down(self, mocker):
        """
        For python interpreters older than 3.4, we know that it sets
        all objects leading with an underscore, to None. threading.Event.wait
        depends on _time(), which has an underscore, leading us to raise an
        exception when wait() is called during shutdown. See

         https://github.com/learningequality/kolibri/issues/1786#issuecomment-313754844


        for a fuller explanation.

        The test here is to see if InfiniteLoopThread (and by extension BaseCloseableThread)
        can handle threading._time being None. We import InfiniteLoopThread
        because it's easier to test.
        """

        ev = threading.Event()
        with mocker.patch.object(threading, "_time"):
            t = InfiniteLoopThread(lambda: id(1), thread_name='test')
            t.start()
            time.sleep(1)
