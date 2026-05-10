-- ══════════════════════════════════════════════════════════════════
--  Smart City Traffic Pipeline — PostgreSQL Schema
--  Database: smartcity
-- ══════════════════════════════════════════════════════════════════

-- ── Raw traffic events (optional sink for debugging) ──────────────
CREATE TABLE IF NOT EXISTS traffic_raw (
    id              SERIAL PRIMARY KEY,
    sensor_id       VARCHAR(10)    NOT NULL,
    junction_name   VARCHAR(100),
    event_time      TIMESTAMPTZ    NOT NULL,
    vehicle_count   INTEGER        NOT NULL,
    avg_speed       NUMERIC(6,2)   NOT NULL,
    is_critical     BOOLEAN        DEFAULT FALSE,
    ingested_at     TIMESTAMPTZ    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_traffic_raw_sensor   ON traffic_raw (sensor_id);
CREATE INDEX IF NOT EXISTS idx_traffic_raw_time     ON traffic_raw (event_time);

-- ── 5-minute windowed congestion index (from Spark) ───────────────
CREATE TABLE IF NOT EXISTS congestion_index (
    id                  SERIAL PRIMARY KEY,
    sensor_id           VARCHAR(10)    NOT NULL,
    junction_name       VARCHAR(100),
    window_start        TIMESTAMPTZ    NOT NULL,
    window_end          TIMESTAMPTZ    NOT NULL,
    mean_speed_kmph     NUMERIC(6,2),
    total_vehicles      INTEGER,
    congestion_index    NUMERIC(6,2),
    congestion_level    VARCHAR(20),   -- LOW / MODERATE / SEVERE
    recorded_at         TIMESTAMPTZ    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ci_sensor  ON congestion_index (sensor_id);
CREATE INDEX IF NOT EXISTS idx_ci_window  ON congestion_index (window_start);

-- ── Critical traffic alerts ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS critical_alerts (
    id              SERIAL PRIMARY KEY,
    sensor_id       VARCHAR(10)    NOT NULL,
    junction_name   VARCHAR(100),
    avg_speed       NUMERIC(6,2)   NOT NULL,
    vehicle_count   INTEGER,
    alert_time      TIMESTAMPTZ    NOT NULL,
    processing_time TIMESTAMPTZ,
    severity        VARCHAR(20)    DEFAULT 'CRITICAL',
    acknowledged    BOOLEAN        DEFAULT FALSE,
    created_at      TIMESTAMPTZ    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_sensor ON critical_alerts (sensor_id);
CREATE INDEX IF NOT EXISTS idx_alerts_time   ON critical_alerts (alert_time);

-- ── Nightly batch report store ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_peak_report (
    id              SERIAL PRIMARY KEY,
    report_date     DATE           NOT NULL,
    sensor_id       VARCHAR(10)    NOT NULL,
    junction_name   VARCHAR(100),
    peak_hour       INTEGER,              -- 0-23
    total_vehicles  INTEGER,
    avg_speed       NUMERIC(6,2),
    avg_ci          NUMERIC(6,2),
    needs_police    BOOLEAN        DEFAULT FALSE,
    reason          TEXT,
    created_at      TIMESTAMPTZ    DEFAULT NOW(),
    UNIQUE (report_date, sensor_id)
);

-- ── Sample seed data for demonstration ────────────────────────────
-- (Useful for testing Airflow DAG without live Spark data)
INSERT INTO congestion_index
    (sensor_id, junction_name, window_start, window_end, mean_speed_kmph, total_vehicles, congestion_index, congestion_level)
VALUES
    ('J001','Galle Face - Marine Drive',   '2025-07-15 07:00:00+05:30','2025-07-15 07:05:00+05:30', 18.5, 210, 72.3, 'SEVERE'),
    ('J001','Galle Face - Marine Drive',   '2025-07-15 08:00:00+05:30','2025-07-15 08:05:00+05:30', 12.1, 280, 85.1, 'SEVERE'),
    ('J001','Galle Face - Marine Drive',   '2025-07-15 17:00:00+05:30','2025-07-15 17:05:00+05:30', 15.3, 250, 78.0, 'SEVERE'),
    ('J002','Pettah - Manning Market',     '2025-07-15 07:00:00+05:30','2025-07-15 07:05:00+05:30', 22.0, 190, 55.3, 'MODERATE'),
    ('J002','Pettah - Manning Market',     '2025-07-15 08:30:00+05:30','2025-07-15 08:35:00+05:30', 10.5, 310, 89.2, 'SEVERE'),
    ('J003','Colombo 3 - Union Place',     '2025-07-15 07:30:00+05:30','2025-07-15 07:35:00+05:30', 38.0,  90, 22.5, 'LOW'),
    ('J003','Colombo 3 - Union Place',     '2025-07-15 17:30:00+05:30','2025-07-15 17:35:00+05:30', 25.0, 130, 47.2, 'MODERATE'),
    ('J004','Nugegoda - High Level Road',  '2025-07-15 07:00:00+05:30','2025-07-15 07:05:00+05:30', 20.0, 230, 67.8, 'SEVERE'),
    ('J004','Nugegoda - High Level Road',  '2025-07-15 17:30:00+05:30','2025-07-15 17:35:00+05:30', 17.5, 260, 74.5, 'SEVERE');

INSERT INTO critical_alerts
    (sensor_id, junction_name, avg_speed, vehicle_count, alert_time, severity)
VALUES
    ('J001', 'Galle Face - Marine Drive',  7.2, 120, '2025-07-15 08:15:00+05:30', 'CRITICAL'),
    ('J002', 'Pettah - Manning Market',    5.1, 145, '2025-07-15 08:35:00+05:30', 'CRITICAL'),
    ('J004', 'Nugegoda - High Level Road', 8.9, 110, '2025-07-15 17:45:00+05:30', 'CRITICAL');