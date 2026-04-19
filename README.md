# firstPetProject

## Стек
- Apache Airflow (CeleryExecutor)
- ClickHouse
- PySpark
- Python
- PostgreSQL (метаданные Airflow)

## Запуск

1. Клонировать репо
2. Скопировать `.env.example` в `.env` и заполнить
3. Создать папки:

   mkdir -p dags logs data clickhouse_data config
4. docker compose up -d

## Структура
- dags/         — Airflow DAG-и
- scripts/      — Python и PySpark скрипты
- data/         — данные (папка лежит в gitignore-е)


В дополнение в папке scripts лежит sql-скрипт с DDL для БДшки в кликхаусе