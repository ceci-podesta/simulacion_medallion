# simulacion_medallion

Pipeline práctico que combina Airflow, pandas y dbt para demostrar la arquitectura medallion (Bronze → Silver → Gold) sobre DuckDB.

## Arquitectura

1. **Bronze → Silver (pandas)**: el DAG ejecuta `scripts/create_silver_orders.py`, que lee `data/raw/raw_orders.csv`, filtra por la fecha del DAG (`ds`) y escribe un CSV limpio en `data/silver/<YYYY-MM-DD>/`.
2. **Silver → Gold (dbt)**: las tareas `dbt run` y `dbt test` utilizan `dbtRunner` con un `var` `silver_path` que apunta al CSV generado para materializar los modelos Bronze/Silver/Gold en DuckDB.
3. **Calidad de datos**: el task `dbt_test` persiste un JSON por fecha en `reports/dq_status_<ds>.json` con el resultado de cada test.

## Estructura actual

```
simulacion_medallion/
├── airflow/
│   ├── airflow.cfg
│   ├── airflow.db
│   ├── dags/medallion_pipeline.py
│   └── simple_auth_manager_passwords.json.generated
├── data/
│   ├── raw/raw_orders.csv
│   └── silver/<ds>/orders_clean_<ds>.csv
├── dbt_medallion/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── profiles.yml (usado vía `--profiles-dir ..`)
│   └── target/, logs/, etc.
├── reports/                      # JSON de data quality por corrida
├── scripts/create_silver_orders.py
├── warehouse.duckdb              # base DuckDB usada por dbt
└── requirements.txt
```

## Requisitos

Ver `requirements.txt` para las versiones exactas (Airflow 3.1, dbt 1.10, DuckDB 1.4, pandas 2.3). Recomendado usar Python 3.12+ y un virtualenv dedicado:

```bash
python -m venv airflow_env
source airflow_env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Puesta en marcha

1. **Inicializar Airflow**
	```bash
	export AIRFLOW_HOME=$(pwd)/airflow
	airflow db migrate
	airflow standalone  # generará Credenciales admin en airflow/simple_auth_manager_passwords.json.generated
	```
	Abrir `http://localhost:8080`, activar el DAG `medallion_pipeline` y dejar que corra el catchup diario.

2. **Ejecución manual (opcional)**
	```bash
	source airflow_env/bin/activate
	python scripts/create_silver_orders.py --execution-date 2018-01-02 \
	  --raw-path data/raw/raw_orders.csv \
	  --output-dir data/silver/2018-01-02

	cd dbt_medallion
	dbt run --vars "silver_path: 'data/silver/2018-01-02/orders_clean_2018-01-02.csv'"
	dbt test --vars "silver_path: 'data/silver/2018-01-02/orders_clean_2018-01-02.csv'"
	```

3. **Documentación de dbt**
	```bash
	cd dbt_medallion
	dbt docs generate
	dbt docs serve --port 8081 --no-browser  # 8080 lo usa Airflow
	```

## Capas medallion en dbt

- `models/bronze`: staging de la data Silver (limpieza mínima/adaptación de tipos).
- `models/silver`: reservado para transforms intermedias (por ahora placeholder con `.gitkeep`).
- `models/gold`: modelos listos para analítica, e.g. `orders_by_status`.

Cada capa cuenta con sus propios `schema.yml` para documentar tests y descripciones en los modelos correspondientes.
