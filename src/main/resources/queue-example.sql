CREATE TABLE queue_task (
  id            BIGSERIAL PRIMARY KEY,
  queue_name    VARCHAR(128) NOT NULL,
  task          TEXT,
  create_time   TIMESTAMP WITH TIME ZONE DEFAULT now(),
  process_time  TIMESTAMP WITH TIME ZONE DEFAULT now(),
  attempt       INTEGER                  DEFAULT 0,
  actor         VARCHAR(128),
  log_timestamp VARCHAR(128)
);

CREATE INDEX queue_task_name_time_desc_idx
  ON queue_task (queue_name, process_time, id DESC);

CREATE INDEX queue_task_name_actor_desc_idx
  ON queue_task (actor, queue_name)

