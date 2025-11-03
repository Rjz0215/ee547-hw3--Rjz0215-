DROP TABLE IF EXISTS stop_events CASCADE;
DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS line_stops CASCADE;
DROP TABLE IF EXISTS stops CASCADE;
DROP TABLE IF EXISTS lines CASCADE;

CREATE TABLE lines (
    line_id      BIGSERIAL PRIMARY KEY,
    line_name    VARCHAR(50) NOT NULL UNIQUE,
    vehicle_type VARCHAR(10) NOT NULL,
    CONSTRAINT vehicle_type_chk CHECK (vehicle_type IN ('rail','bus'))
);

CREATE TABLE stops (
    stop_id   BIGSERIAL PRIMARY KEY,
    stop_name VARCHAR(100) NOT NULL UNIQUE,
    latitude  NUMERIC(9,6)  NOT NULL,
    longitude NUMERIC(9,6)  NOT NULL,
    CONSTRAINT lat_chk CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT lon_chk CHECK (longitude BETWEEN -180 AND 180)
);

CREATE TABLE line_stops (
    line_id             BIGINT  NOT NULL REFERENCES lines(line_id) ON DELETE RESTRICT,
    stop_id             BIGINT  NOT NULL REFERENCES stops(stop_id) ON DELETE RESTRICT,
    sequence_number     INTEGER NOT NULL,
    time_offset_minutes INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (line_id, stop_id),
    CONSTRAINT sequence_positive CHECK (sequence_number >= 1),
    CONSTRAINT offset_nonneg     CHECK (time_offset_minutes >= 0),
    CONSTRAINT sequence_unique_per_line UNIQUE (line_id, sequence_number)
);

CREATE TABLE trips (
    trip_code           VARCHAR(20) PRIMARY KEY,
    line_id             BIGINT NOT NULL REFERENCES lines(line_id) ON DELETE RESTRICT,
    scheduled_departure TIMESTAMP NOT NULL,
    vehicle_id          VARCHAR(30) NOT NULL
);

CREATE TABLE stop_events (
    event_id       BIGSERIAL PRIMARY KEY,
    trip_code      VARCHAR(20) NOT NULL REFERENCES trips(trip_code) ON DELETE RESTRICT,
    stop_id        BIGINT      NOT NULL REFERENCES stops(stop_id) ON DELETE RESTRICT,
    scheduled_time TIMESTAMP   NOT NULL,
    actual_time    TIMESTAMP   NOT NULL,
    passengers_on  INTEGER     NOT NULL DEFAULT 0,
    passengers_off INTEGER     NOT NULL DEFAULT 0,
    CONSTRAINT pax_on_nonneg  CHECK (passengers_on  >= 0),
    CONSTRAINT pax_off_nonneg CHECK (passengers_off >= 0)
);

CREATE INDEX idx_line_stops_line_seq ON line_stops(line_id, sequence_number);
CREATE INDEX idx_line_stops_stop     ON line_stops(stop_id);
CREATE INDEX idx_trips_line          ON trips(line_id, scheduled_departure);
CREATE INDEX idx_stop_events_trip    ON stop_events(trip_code, scheduled_time);
CREATE INDEX idx_stop_events_stop    ON stop_events(stop_id);
