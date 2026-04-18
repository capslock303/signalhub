from __future__ import annotations

SCHEMA_VERSION = 2

DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sensors (
  sensor_id TEXT PRIMARY KEY,
  sensor_type TEXT NOT NULL,
  model TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS capture_sessions (
  session_id TEXT PRIMARY KEY,
  sensor_id TEXT NOT NULL REFERENCES sensors(sensor_id),
  started_at REAL,
  ended_at REAL,
  source_path TEXT NOT NULL,
  source_sha256 TEXT,
  imported_at REAL NOT NULL,
  notes TEXT,
  environment_tag TEXT
);

CREATE TABLE IF NOT EXISTS ble_observations (
  observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL REFERENCES capture_sessions(session_id),
  timestamp REAL,
  address TEXT,
  address_type TEXT,
  pdu_type TEXT,
  rssi REAL,
  name_hint TEXT,
  manufacturer_hint TEXT,
  service_hint TEXT,
  connection_seen INTEGER NOT NULL DEFAULT 0,
  gatt_seen INTEGER NOT NULL DEFAULT 0,
  smp_seen INTEGER NOT NULL DEFAULT 0,
  encrypted_seen INTEGER NOT NULL DEFAULT 0,
  frame_protocols TEXT,
  raw_ref TEXT
);

CREATE INDEX IF NOT EXISTS idx_ble_obs_session ON ble_observations(session_id);
CREATE INDEX IF NOT EXISTS idx_ble_obs_addr ON ble_observations(address);
CREATE INDEX IF NOT EXISTS idx_ble_obs_ts ON ble_observations(timestamp);
CREATE INDEX IF NOT EXISTS idx_ble_obs_name_hint ON ble_observations(name_hint);

CREATE TABLE IF NOT EXISTS ble_devices (
  ledger_id TEXT PRIMARY KEY,
  stable_identity_key TEXT NOT NULL UNIQUE,
  first_seen REAL,
  last_seen REAL,
  most_recent_session_id TEXT,
  address TEXT,
  address_type TEXT,
  identity_kind TEXT NOT NULL DEFAULT 'mac',
  primary_pdu_types TEXT,
  connectable TEXT,
  scannable TEXT,
  connection_seen INTEGER NOT NULL DEFAULT 0,
  gatt_seen INTEGER NOT NULL DEFAULT 0,
  smp_seen INTEGER NOT NULL DEFAULT 0,
  encrypted_seen INTEGER NOT NULL DEFAULT 0,
  current_name_hint TEXT,
  current_manufacturer_hint TEXT,
  current_service_hint TEXT,
  rssi_min REAL,
  rssi_max REAL,
  appearance_pattern TEXT,
  probable_device_class TEXT,
  confidence TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS ble_device_session_summary (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ledger_id TEXT REFERENCES ble_devices(ledger_id),
  session_id TEXT NOT NULL REFERENCES capture_sessions(session_id),
  address TEXT,
  first_seen REAL,
  last_seen REAL,
  rssi_min REAL,
  rssi_max REAL,
  pdu_summary TEXT,
  connection_seen INTEGER NOT NULL DEFAULT 0,
  gatt_seen INTEGER NOT NULL DEFAULT 0,
  smp_seen INTEGER NOT NULL DEFAULT 0,
  encrypted_seen INTEGER NOT NULL DEFAULT 0,
  appearance_pattern TEXT,
  packet_count INTEGER,
  UNIQUE(session_id, address)
);

CREATE INDEX IF NOT EXISTS idx_ble_dss_session ON ble_device_session_summary(session_id);
CREATE INDEX IF NOT EXISTS idx_ble_dss_ledger ON ble_device_session_summary(ledger_id);

CREATE TABLE IF NOT EXISTS ble_aliases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ledger_id TEXT NOT NULL REFERENCES ble_devices(ledger_id),
  alias_type TEXT NOT NULL,
  alias_value TEXT NOT NULL,
  first_seen REAL,
  last_seen REAL
);

CREATE INDEX IF NOT EXISTS idx_ble_aliases_ledger ON ble_aliases(ledger_id);
"""
