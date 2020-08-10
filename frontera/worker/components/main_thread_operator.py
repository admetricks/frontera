import threading

from six.moves import queue

from frontera.worker.components import DBWorkerPeriodicComponent


class MainThreadOperator(DBWorkerPeriodicComponent):
    """Executes code in main thread to perform backend operations."""

    NAME = "main_thread_operator"

    def __init__(self, worker, settings, stop_event, *args, **kwargs):
        super(MainThreadOperator, self).__init__(worker, settings, stop_event, *args, **kwargs)
        self.queue = queue.Queue()
        self._closed = threading.Event()

    def perform(self, function, shutdown_value=None):
        """Executes a function on the main thread when called from another one.

        Do not call on the main thread, unless you want a deadlock.

        If called when the slot is closed (i.e. when shutdown has started), it won't
        do anything but to return a shutdown_value passed (None by default)

        Args:
            - function: The function to execute
            - shutdown_value: Value to be returned if called when component is closed

        Returns:
            The function result after having run it on the main thread (blocking).
        """
        if self._closed.is_set():
            return shutdown_value
        job = MainThreadOperatorJob(function)
        self.queue.put(job)
        job.event.wait()
        return job.result

    def schedule(self, delay=0.1):
        self.periodic_task.schedule(delay)

    def run_and_reschedule(self):
        if not self.stopped:
            self.run()
            self.periodic_task.schedule(0.1)

    def run_errback(self, failure):
        self.logger.error(failure.getTraceback())
        if not self.stopped:
            self.periodic_task.schedule(0.1)

    def run(self):
        jobs = []
        try:
            while True:
                jobs.append(self.queue.get(False))
        except queue.Empty:
            pass
        for job in jobs:
            result = job.function()
            job.result = result
            job.event.set()

    @property
    def stopped(self):
        return self._closed.is_set()

    def close(self):
        self._closed.set()  # Don't accept more jobs
        self.run()  # Finish last pending jobs


class MainThreadOperatorJob:
    def __init__(self, function):
        self.function = function
        self.event = threading.Event()
        self.result = None
