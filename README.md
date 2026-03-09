# PyETLite

> Declarative ETL pipelines in Python, powered by Polars.

```python
from pyetlite import Pipeline, ErrorMode
from pyetlite.sources import PostgresSource
from pyetlite.sinks import CSVSink
from pyetlite.transforms import DropNulls, RenameColumns

result = (
    Pipeline("ventas_etl", error_mode=ErrorMode.SKIP_AND_LOG)
    .extract(PostgresSource(conn="postgres://...", query="SELECT * FROM orders"))
    .transform(DropNulls())
    .transform(RenameColumns({"id": "order_id"}))
    .load(CSVSink("output/orders.csv"))
    .run()
)
print(result.summary())
```

## Instalación

```bash
pip install pyetlite

# Con soporte de base de datos
pip install "pyetlite[postgres]"
pip install "pyetlite[mysql]"
```

## Desarrollo local

```bash
# 1. Clonar el repo
git clone https://github.com/tu-usuario/pyetlite
cd pyetlite

# 2. Instalar hatch
pip install hatch

# 3. Crear entorno y correr tests
hatch run dev:test

# 4. Lint
hatch run dev:lint
```

## Licencia

MIT
