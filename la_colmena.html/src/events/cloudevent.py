import json
import uuid
from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class CloudEvent:
    specversion: str
    id: str
    source: str
    type: str
    datacontenttype: str
    data: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "specversion": self.specversion,
            "id": self.id,
            "source": self.source,
            "type": self.type,
            "datacontenttype": self.datacontenttype,
            "data": self.data,
        }

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )


def crear_cloudevent(
    tipo: str,
    datos: Dict[str, Any],
    source: str = "bina/la-colmena",
) -> CloudEvent:
    if not isinstance(tipo, str) or not tipo.strip():
        raise ValueError("El tipo del CloudEvent es obligatorio.")
    if not isinstance(source, str) or not source.startswith("bina"):
        raise ValueError("El origen del CloudEvent debe comenzar con 'bina'.")
    if not isinstance(datos, dict):
        raise ValueError("Los datos del CloudEvent deben ser un objeto JSON.")

    event = CloudEvent(
        specversion="1.0",
        id=str(uuid.uuid4()),
        source=source,
        type=tipo,
        datacontenttype="application/json",
        data=datos,
    )
    event.to_json()
    return event
