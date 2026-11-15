from .cloudevent import CloudEvent, crear_cloudevent
from .bus import InMemoryEventBus

# Singleton global - toda la app usa el mismo bus
event_bus_global = InMemoryEventBus()

__all__ = ['CloudEvent', 'crear_cloudevent', 'InMemoryEventBus', 'event_bus_global']