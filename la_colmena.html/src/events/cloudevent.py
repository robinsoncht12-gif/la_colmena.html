import uuid
import datetime
from dataclasses import dataclass, field, asdict
from typing import Any, Dict

@dataclass
class CloudEvent:
    specversion: str = "1.0"
    type: str = ""
    source: str = "LaColmena"
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    data: Dict[str, Any] = field(default_factory=dict)
    time: str = field(default_factory=lambda: datetime.datetime.utcnow().isoformat() + "Z")
    idempotency_key: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self):
        return asdict(self)

def crear_cloudevent(tipo: str, datos: dict, source: str = "LaColmena") -> CloudEvent:
    return CloudEvent(
        type=tipo,
        source=source,
        data=datos,
        id=str(uuid.uuid4()),
        idempotency_key=str(uuid.uuid4())
    )