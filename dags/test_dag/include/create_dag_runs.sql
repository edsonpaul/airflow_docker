CREATE TABLE IF NOT EXISTS dag_runs (
    dt date,
    dag_id character varying,
    primary key(dt, dag_id)
)