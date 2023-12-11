--Partitioning methods
CREATE TABLE measurements (
  id int8 NOT NULL,
  value float8 NOT NULL,
  date timestamptz NOT NULL
);

CREATE TABLE measurements (
  id int8 NOT NULL,
  value float8 NOT NULL,
  date timestamptz NOT NULL
) PARTITION BY RANGE (date);

--Partition by range
CREATE TABLE measurements_y2021m01 PARTITION OF measurements
FOR VALUES FROM ('2021-01-01') TO ('2021-02-01');

-- Partition by list
CREATE TABLE measurements (
  id int8 PRIMARY KEY,
  value float8 NOT NULL,
  date timestamptz NOT NULL,
  hot boolean
) PARTITION BY LIST (hot);

CREATE TABLE measurements_hot PARTITION OF measurements
FOR VALUES IN (TRUE);

CREATE TABLE measurements_cold PARTITION OF measurements
FOR VALUES IN (NULL);

-- Move rows to measurements_hot
UPDATE measurements SET hot = TRUE;

-- Move rows to measurements_cold
UPDATE measurements SET hot = NULL;

--Partition by hash
CREATE TABLE measurements (
  id int8 PRIMARY KEY,
  value float8 NOT NULL,
  date timestamptz NOT NULL
) PARTITION BY HASH (id);

CREATE TABLE measurements_1 PARTITION OF measurements
FOR VALUES WITH (MODULUS 3, REMAINDER 0);

CREATE TABLE measurements_2 PARTITION OF measurements
FOR VALUES WITH (MODULUS 3, REMAINDER 1);

CREATE TABLE measurements_3 PARTITION OF measurements
FOR VALUES WITH (MODULUS 3, REMAINDER 2);

--Managing partitions
ALTER TABLE measurements DETACH PARTITION measurements_y2021m01;

ALTER TABLE measurements ATTACH PARTITION measurements_y2021m01
FOR VALUES FROM ('2021-01-01') TO ('2021-02-01');

-- Use the existing table as a partition for the existing data.
ALTER TABLE measurements RENAME TO measurements_y2021m01;

-- Create the partitioned table.
CREATE TABLE measurements (LIKE measurements_y2021m01 INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
PARTITION BY RANGE (date);

-- Attach the existing partition with open left constraint.
ALTER TABLE measurements ATTACH PARTITION measurements_y2021m01
FOR VALUES FROM ('0001-01-01') TO ('2021-02-01');

-- Use proper constraints for new partitions.
CREATE TABLE measurements_y2021m02 PARTITION OF measurements
FOR VALUES FROM ('2021-02-01') TO ('2021-03-01');
