"""
========================================================================================
MÓDULO DE VALIDACIONES DE ENTRADA, INTEGRIDAD Y NEGOCIO (FAIL-FAST)
Cátedra: Ingeniería de Software 3 [IF0129] - Corporación Universitaria Remington
Patrón: Validator / Specification, Defensiveness, SoD (Separación de Responsabilidades)
Dependencias: NINGUNA (Python Standard Library `re`, `typing`)
========================================================================================
"""
import re
from typing import List, Dict, Any

class ValidationError(ValueError):
    """Excepción específica para violaciones de validación de negocio y sintaxis."""
    def __init__(self, mensaje: str, campo: str = ""):
        super().__init__(mensaje)
        self.campo = campo


class Validator:
    """
    Motor central de validación previa al procesamiento en base de datos.
    Aplica principio Fail-Fast para impedir estados inconsistentes.
    """

    @staticmethod
    def validar_no_vacio(valor: str, nombre_campo: str) -> str:
        """Verifica que el string no sea nulo ni consista únicamente de espacios."""
        if not valor or not str(valor).strip():
            raise ValidationError(f"El campo '{nombre_campo}' es obligatorio y no puede estar vacío.", campo=nombre_campo)
        return str(valor).strip()

    @staticmethod
    def validar_cliente_id(cliente_id: str) -> str:
        """
        Valida que el identificador de cliente cumpla con el formato esperado:
        Cédula de ciudadanía o NIT alfanumérico seguro (5 a 20 caracteres sin inyecciones).
        """
        cid = Validator.validar_no_vacio(cliente_id, "cliente_id")
        if not re.match(r"^[A-Za-z0-9\-_]{4,25}$", cid):
            raise ValidationError(
                f"El identificador de cliente '{cid}' contiene caracteres inválidos. Debe ser alfanumérico entre 4 y 25 caracteres.",
                campo="cliente_id"
            )
        return cid

    @staticmethod
    def validar_idempotency_key(key: str) -> str:
        """
        Valida que la clave de idempotencia sea un token no vacío, sin espacios y seguro.
        """
        k = Validator.validar_no_vacio(key, "idempotency_key")
        if len(k) < 8 or len(k) > 120:
            raise ValidationError(
                f"La idempotency_key debe tener entre 8 y 120 caracteres. Longitud actual: {len(k)}",
                campo="idempotency_key"
            )
        if any(character.isspace() for character in k):
            raise ValidationError(
                "La idempotency_key no puede contener espacios.",
                campo="idempotency_key",
            )
        return k

    @staticmethod
    def validar_items_pedido(items_raw: Any) -> List[Dict[str, Any]]:
        """
        Valida la estructura de la lista de productos solicitados:
        - Debe ser una lista no vacía.
        - Cada item debe contener 'codigo' válido y 'cantidad' entera > 0.
        """
        if not isinstance(items_raw, list) or len(items_raw) == 0:
            raise ValidationError("El pedido debe contener al menos un producto (lista 'items' no vacía).", campo="items")

        items_validados = []
        codigos_vistos = set()

        for idx, item in enumerate(items_raw):
            if not isinstance(item, dict):
                raise ValidationError(f"El elemento {idx} de la lista de items no es un objeto válido.", campo=f"items[{idx}]")

            codigo = item.get("codigo")
            cantidad = item.get("cantidad")

            if not codigo or not isinstance(codigo, str) or not codigo.strip():
                raise ValidationError(f"El item en posición {idx} no especifica un 'codigo' válido.", campo=f"items[{idx}].codigo")
            
            codigo_limpio = codigo.strip().upper()

            if codigo_limpio in codigos_vistos:
                raise ValidationError(f"Producto duplicado '{codigo_limpio}' en la misma orden. Consolide las cantidades.", campo="items")
            codigos_vistos.add(codigo_limpio)

            try:
                cant_int = int(cantidad)
            except (ValueError, TypeError):
                raise ValidationError(f"La cantidad para el producto '{codigo_limpio}' debe ser un número entero.", campo=f"items[{idx}].cantidad")

            if cant_int <= 0:
                raise ValidationError(f"La cantidad para '{codigo_limpio}' debe ser estrictamente mayor a 0 (recibido: {cant_int}).", campo=f"items[{idx}].cantidad")
            if cant_int > 500:
                raise ValidationError(f"La cantidad ({cant_int}) para '{codigo_limpio}' excede el límite máximo de venta por orden minorista (500).", campo=f"items[{idx}].cantidad")

            items_validados.append({
                "codigo": codigo_limpio,
                "cantidad": cant_int
            })

        return items_validados

    @staticmethod
    def validar_monto(monto: float, nombre_campo: str = "monto") -> float:
        """Valida que un valor monetario sea positivo y finito."""
        try:
            m = float(monto)
        except (ValueError, TypeError):
            raise ValidationError(f"El campo '{nombre_campo}' debe ser numérico.", campo=nombre_campo)

        if m <= 0.0:
            raise ValidationError(f"El campo '{nombre_campo}' debe ser mayor a cero. Recibido: {m}", campo=nombre_campo)
        return round(m, 2)
