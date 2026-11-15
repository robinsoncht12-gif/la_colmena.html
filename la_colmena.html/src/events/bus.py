import threading
import queue
from collections import defaultdict
from typing import Callable

class InMemoryEventBus:
    def __init__(self):
        self._subs = defaultdict(list)
        self._queue = queue.Queue()
        self._running = True
        self._worker = threading.Thread(target=self._process, daemon=True)
        self._worker.start()

    def subscribe(self, event_type: str, handler: Callable):
        self._subs[event_type].append(handler)

    def publish(self, event):
        self._queue.put(event)
        return True

    def _process(self):
        while self._running:
            try:
                event = self._queue.get(timeout=0.1)
                event_type = getattr(event, 'type', '')
                for h in self._subs.get(event_type, []):
                    try:
                        h(event)
                    except:
                        pass
            except queue.Empty:
                continue

event_bus_global = None