import clickhouse_connect
import os

client = clickhouse_connect.get_client(
    host=os.getenv("CH_HOST", "clickhouse"),
    port=int(os.getenv("CH_PORT", 8123)),
    username=os.getenv("CH_USER", "airflow"),
    password=os.getenv("CH_PASSWORD", "airflow")
)
df = client.query("""
        SELECT house_id, square
        FROM airflow.russian_houses
        WHERE square > 60.0
        ORDER BY square DESC
        LIMIT 25
""").result_set

print(df)