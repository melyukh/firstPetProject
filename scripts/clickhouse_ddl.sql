--запрос для создания таблицы в clickhouss
CREATE TABLE airflow.russian_houses
(
    house_id Int32,
    latitude Nullable(Float32),
    longitude Nullable(Float32),
    maintenance_year Nullable(Int32),
    square Nullable(Float32),
    population Nullable(Int64),
    region Nullable(String),
    locality_name Nullable(String),
    address Nullable(String),
    full_address Nullable(String),
    communal_service_id Nullable(Int32),
    description Nullable(String)
)
ENGINE = MergeTree()
ORDER BY house_id