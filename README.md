# simulacion_medallion

Proyecto de práctica para el parcial: pipeline que combina Airflow + dbt siguiendo una arquitectura medallion.

## Objetivo

1. **Bronze → Silver**: una tarea de Airflow lee `data/raw/raw_orders.csv`, filtra según la fecha de ejecución y aplica limpieza básica con pandas para generar un archivo limpio (Silver).
2. **Silver → Gold**: un proyecto dbt carga el archivo limpio en DuckDB y materializa tablas finales.
3. **Calidad de datos**: se ejecutan tests de dbt y se guarda un reporte con el resultado (pass/fail) para cada corrida.

## Estructura inicial

```
simulacion_medallion/
├── data/
│   └── raw/
│       └── raw_orders.csv
├── airflow/              # (se creará) DAGs y utilidades de Airflow
├── dbt/                  # (se creará) proyecto dbt inicializado con `dbt init`
├── scripts/              # (se creará) helpers para ejecutar dbt desde Python
└── README.md
```

Próximos pasos:
- Inicializar el proyecto dbt dentro de `dbt/` apuntando a DuckDB.
- Crear el DAG en `airflow/dags/` con las tres tasks (limpieza, dbt run, dbt test).
- Automatizar la ejecución programática de dbt y generar el archivo de resultado de tests.
