CREATE TABLE IF NOT EXISTS pipelines (
    ID SERIAL PRIMARY KEY,
    dag_name TEXT,
    run_id TEXT,
    run_date TEXT NOT NULL,
    start_time TEXT NOT NULL,
    finish_time TEXT,
    nodes TEXT[] NOT NULL,
    inputs TEXT[],
    outputs TEXT[],
    parameters TEXT[],
    tags TEXT[],
    namespace TEXT
);

CREATE TABLE IF NOT EXISTS nodes (
    ID INT,
    run_id TEXT,
    run_date TEXT NOT NULL,
    start_time TEXT NOT NULL,
    finish_time TEXT,
    func TEXT,
    inputs TEXT[],
    outputs TEXT[],
    name TEXT,
    tags TEXT[],
    confirms TEXT[],
    namespace TEXT,
    status TEXT,
    detail TEXT,
    PRIMARY KEY (ID, name),
    FOREIGN KEY (ID) REFERENCES pipelines(ID) ON DELETE CASCADE
);