CREATE SCHEMA history;

CREATE TYPE history.status AS ENUM ('offline', 'online', 'busy');
CREATE TYPE history.order_status AS ENUM ('none', 'driving', 'waiting',
  'transporting', 'complete', 'failed', 'cancelled', 'preexpired', 'expired',
  'unknown');

CREATE TYPE history.event_tuple AS (
    event_ts TIMESTAMP WITH TIME ZONE,
    status history.status,
    order_statuses history.order_status[]
    );

CREATE TYPE history.contractor_id_tuple AS (
    park_id VARCHAR(48),
    profile_id VARCHAR(48)
    );

CREATE TYPE history.status_event_tuple AS (
    park_id VARCHAR(48),
    profile_id VARCHAR(48),
    event_list history.event_tuple[]
    );

CREATE TABLE history.events(
    park_id VARCHAR(48) NOT NULL,
    profile_id VARCHAR(48) NOT NULL,
    event_list history.event_tuple[],
    contractor_id_tuple_val history.contractor_id_tuple,
    CONSTRAINT event_pkey PRIMARY KEY (profile_id,park_id)
);

INSERT INTO history.events (park_id, profile_id, event_list, contractor_id_tuple_val) VALUES ('park3', 'profile3', '{"(\"2023-02-02 11:43:32.335573+03\",online,{driving})"}', '(a,b)');

-- ---

CREATE TYPE my_enum_type AS ENUM('foo', 'bar');

create table table_with_enum
(
    id INT primary key,
    val my_enum_type
);

INSERT INTO table_with_enum (id, val) VALUES (1, 'foo');
