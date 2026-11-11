-- MANUAL additive migration. NOT under classpath db/migration; no automatic runner.
-- Verify public.requests(id bigint) and the intended database before executing once.
BEGIN;
SET LOCAL lock_timeout = '5s';
CREATE SCHEMA IF NOT EXISTS bd_requests_kafka;
CREATE TABLE bd_requests_kafka.schema_version (
    version integer PRIMARY KEY,
    installed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE bd_requests_kafka.stream (
    request_id bigint PRIMARY KEY REFERENCES public.requests(id),
    tracking_status varchar(30) NOT NULL CHECK (tracking_status IN ('ACTIVE','BLOCKED_HISTORY')),
    last_version bigint NOT NULL DEFAULT 0 CHECK (last_version >= 0),
    last_status varchar(30) NOT NULL,
    last_occurred_at timestamptz NOT NULL,
    procedure_id varchar(120) NOT NULL,
    procedure_name varchar(250),
    blocked_reason varchar(120),
    CHECK (procedure_name IS NULL OR length(trim(procedure_name)) > 0)
);
CREATE TABLE bd_requests_kafka.outbox (
    event_id uuid PRIMARY KEY,
    request_id bigint NOT NULL REFERENCES bd_requests_kafka.stream(request_id),
    request_version bigint NOT NULL CHECK (request_version > 0),
    event_type varchar(40) NOT NULL CHECK (event_type IN ('REQUEST_CREATED','REQUEST_STATUS_CHANGED')),
    previous_status varchar(30),
    status varchar(30) NOT NULL,
    procedure_id varchar(120) NOT NULL,
    occurred_at timestamptz NOT NULL,
    topic varchar(249) NOT NULL CHECK (topic = 'bd.requests.events.v1'),
    payload text,
    attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    last_error varchar(200),
    UNIQUE(request_id,request_version),
    CHECK ((event_type='REQUEST_CREATED' AND request_version=1 AND previous_status IS NULL AND status='INGRESADO')
        OR (event_type='REQUEST_STATUS_CHANGED' AND request_version>1 AND previous_status IS NOT NULL AND previous_status<>status))
);
CREATE INDEX requests_outbox_pending_idx ON bd_requests_kafka.outbox(available_at,occurred_at) WHERE published_at IS NULL;
CREATE INDEX requests_outbox_order_idx ON bd_requests_kafka.outbox(request_id,request_version) WHERE published_at IS NULL;
-- No final version is assigned to history lacking an authoritative creation chain.
CREATE TABLE bd_requests_kafka.history_hold (
    sequence_id bigserial PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    request_id bigint NOT NULL REFERENCES bd_requests_kafka.stream(request_id),
    procedure_id varchar(120) NOT NULL,
    previous_status varchar(30) NOT NULL,
    status varchar(30) NOT NULL,
    occurred_at timestamptz NOT NULL,
    reason varchar(120) NOT NULL
);
CREATE INDEX requests_history_hold_request_idx ON bd_requests_kafka.history_hold(request_id,sequence_id);
INSERT INTO bd_requests_kafka.schema_version(version) VALUES (1);
COMMIT;
