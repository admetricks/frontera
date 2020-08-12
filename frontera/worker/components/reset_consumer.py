import threading
import time
from timeit import default_timer as timer

from frontera.worker.components import DBWorkerThreadComponent


class ResetConsumer(DBWorkerThreadComponent):

    NAME = 'reset'
    RESET_TIMEOUT = 60

    def __init__(self, worker, settings, stop_event, *args, **kwargs):
        self.others_stop_event = stop_event
        stop_event = threading.Event()
        super(ResetConsumer, self).__init__(worker, settings, stop_event, *args, **kwargs)
        reset_log = worker.message_bus.reset_log()
        reset_ack_log = worker.message_bus.reset_ack_log()
        self.reset_log_consumer = reset_log.consumer()
        self.reset_ack_log_producer = reset_ack_log.producer()
        self.started = threading.Event()
        self.resetting = False
        self.reset_start = None

    def schedule(self):
        if self.started.is_set():
            return
        self.started.set()
        super(ResetConsumer, self).schedule()

    def run(self):
        def backend_reset():
            """Needs to be executed on main thread (sessions aren't thread safe)."""
            self.worker.backend.frontier_stop()
            self.worker.backend.frontier_start()

        reset_done = False
        for m in self.reset_log_consumer.get_messages(count=1, timeout=1):
            try:
                msg = self.worker._decoder.decode(m)
            except (KeyError, TypeError):
                self.logger.exception("Decoding error")
                continue
            else:
                if msg[0] == 'reset':
                    self.logger.info(
                        "Reset signal received. Sending ACK to seeder, "
                        "flushing DB cache and pausing worker."
                    )
                    self.others_stop_event.set()
                    # giving time for other components to die off, just in case
                    time.sleep(5)

                    # flush backend
                    self.worker.on_main_thread(backend_reset)

                    reset_ack_signal = self.worker._encoder.encode_reset_ack()
                    self.reset_ack_log_producer.send(None, reset_ack_signal)
                    self.resetting = True
                    self.reset_start = timer()
                elif msg[0] == 'reset_done':
                    self.logger.info("Got reset_done signal. Resuming worker.")
                    reset_done = True
        if self.resetting and timer() - self.reset_start > self.RESET_TIMEOUT:
            self.logger.warning(
                "Considering reset as done because of timeout, resuming worker"
            )
            reset_done = True
        if reset_done:
            # flush backend again just in case
            self.worker.on_main_thread(backend_reset)
            self.resetting = False
            self.others_stop_event.clear()
            self.worker.slot.schedule()

    def close(self):
        self.stop_event.set()
        self.started.clear()
