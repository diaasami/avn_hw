CREATE TABLE metrics (
  ts timestamp NOT NULL,
  machine_id varchar(256),
  payload JSONB,
  PRIMARY KEY(ts, machine_id)
);