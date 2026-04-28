# IDEAM Hydrology Data Automator

Pipeline en Python para extraer, validar, organizar y preparar datos hidrologicos del
IDEAM publicados en Socrata / Datos Abiertos Colombia.

El proyecto esta orientado a descargas reproducibles, control de calidad basico,
exportacion organizada por territorio y preparacion para upsert hacia un dataset Socrata
destino.

## Funcionalidades

- Consulta datasets IDEAM en `www.datos.gov.co` mediante `sodapy`.
- Filtra por departamento, municipio, estacion y rango temporal.
- Homologa variantes de departamentos como `ATLANTICO` y `ATLANTICO` con tilde.
- Genera `floating_id` estable para upsert idempotente en Socrata.
- Exporta datos a `data/departamento/municipio/`.
- Nombra archivos como `variable_departamento_municipio_hhmm_ddmmyy`.
- Divide CSV grandes con sufijos `_2`, `_3`, etc. para evitar limites de Excel.
- Incluye validacion Pydantic para payloads de carga.
- Incluye una interfaz CLI interactiva y una verificacion rapida para Atlantico.

## Instalacion

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e .
```

Tambien se puede instalar con:

```powershell
pip install -r requirements.txt
```

## Configuracion

Copia `.env.example` a `.env` si quieres manejar variables localmente.
El proyecto no necesita token para todos los endpoints publicos, pero un token de Socrata
mejora limites y estabilidad.

Variables principales:

```text
SOCRATA_APP_TOKEN=
SOCRATA_DOMAIN=www.datos.gov.co
SOCRATA_USERNAME=
SOCRATA_PASSWORD=
SOCRATA_LIMIT=50000
SOCRATA_MAX_WORKERS=10
SOCRATA_TIMEOUT=300
SOCRATA_UPSERT_CHUNK_SIZE=5000
EXCEL_MAX_ROWS=1048576
```

Para upsert real hacia Socrata, configura `SOCRATA_USERNAME` y `SOCRATA_PASSWORD`.
El dataset destino debe tener `floating_id` como identificador unico de fila.

## Uso

Abrir asistente interactivo:

```powershell
ideam-socrata interactive
```

O desde Python:

```powershell
python -m ideam_socrata.cli interactive
```

Verificacion rapida para precipitacion en Atlantico:

```powershell
ideam-socrata verify-atlantico --start-date 2024-01-01 --end-date 2024-02-01
```

## Estructura

```text
src/ideam_socrata/
  cli.py               # Entry point CLI
  config.py            # Configuracion y cliente Socrata
  core.py              # Flujo interactivo de descarga
  extract.py           # Paginacion Socrata
  transform.py         # Normalizacion, floating_id, deduplicacion
  query_validation.py  # Validacion de variantes territoriales
  exporting.py         # Parquet/CSV organizado por carpetas
  validation.py        # Modelos Pydantic
  load.py              # Payload y upsert Socrata
  tools.py             # Funcion/schema para agentes LLM
tests/
  test_query_export_helpers.py
```

## Pruebas

```powershell
python -m unittest discover -s tests
python -m compileall src tests
```

## Politica de datos

No se deben subir datos reales, logs, caches, backups ni credenciales al repositorio.
Los directorios `data/`, `archive/`, `Backup/`, `logs/`, `scratch/`, `scripts/legacy/`
y `src/gui/` estan excluidos del paquete publico por defecto.

## Licencia

MIT. Ver `LICENSE`.
