try:
    from pydantic import BaseModel, Field, ValidationError
except ImportError:
    BaseModel = Field = ValidationError = None


if BaseModel:
    class IdeamObservation(BaseModel):
        """Validated IDEAM row shape for Socrata upsert payloads."""

        floating_id: str
        source_dataset_id: str
        codigoestacion: str
        codigosensor: str | None = None
        fechaobservacion: str
        valorobservado: float | None = None
        nombreestacion: str | None = None
        departamento: str | None = None
        municipio: str | None = None
        zonahidrografica: str | None = None
        latitud: float | None = Field(default=None, ge=-90, le=90)
        longitud: float | None = Field(default=None, ge=-180, le=180)
        descripcionsensor: str | None = None
        unidadmedida: str | None = None
else:
    IdeamObservation = None


def validate_payload(records):
    """Validate records with Pydantic when available; return accepted and rejected rows."""
    if not IdeamObservation:
        return records, []

    accepted = []
    rejected = []
    for idx, record in enumerate(records):
        try:
            validated = IdeamObservation(**record)
            accepted.append(validated.model_dump() if hasattr(validated, "model_dump") else validated.dict())
        except ValidationError as exc:
            rejected.append({"index": idx, "error": str(exc), "record": record})
    return accepted, rejected
