"""Unit tests for fingerprint profiles, crypto metadata, and analytics helpers."""

from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path

from signalhub.ble import advanced_analytics
from signalhub.ble.decrypt_workflow import set_session_secrets_path, upsert_session_crypto_from_observations
from signalhub.ble.identity import (
    fingerprint_identity_key_v1,
    fingerprint_identity_key_v2,
    stable_identity_key_and_kind,
)
from signalhub.ble.multi_sensor import clock_skew_public_mac_pairs
from signalhub.ble.reports import render_insights_report
from signalhub.common.timeutil import utc_day_range_epoch, utc_today_iso
from signalhub.db.sqlite import init_db


def _memory_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    init_db(conn)
    return conn


def _seed_session(conn: sqlite3.Connection, session_id: str, sensor_id: str = "s1") -> None:
    conn.execute(
        "INSERT OR IGNORE INTO sensors(sensor_id, sensor_type) VALUES (?, ?)",
        (sensor_id, "test"),
    )
    conn.execute(
        """
        INSERT INTO capture_sessions(
          session_id, sensor_id, source_path, imported_at
        ) VALUES (?, ?, ?, 1.0)
        """,
        (session_id, sensor_id, "/tmp/x.pcapng"),
    )
    conn.commit()


class FingerprintProfileTests(unittest.TestCase):
    def test_v2_key_differs_when_uuid128_folded(self) -> None:
        u = frozenset({"0000180f-0000-1000-8000-00805f9b34fb"})
        k1 = fingerprint_identity_key_v1(
            name="SensorX",
            manufacturer="Acme",
            service="180F",
        )
        k2_empty = fingerprint_identity_key_v2(
            name="SensorX",
            manufacturer="Acme",
            service="180F",
            uuid128s=frozenset(),
        )
        k2_u = fingerprint_identity_key_v2(
            name="SensorX",
            manufacturer="Acme",
            service="180F",
            uuid128s=u,
        )
        # v2 uses a different payload than v1 even with no UUID hints.
        self.assertNotEqual(k1, k2_empty)
        self.assertNotEqual(k2_empty, k2_u)
        self.assertTrue(k2_u.startswith("fp:v2:"))

    def test_stable_identity_v2_uses_fingerprint_with_uuid_only_name(self) -> None:
        key, kind = stable_identity_key_and_kind(
            address="aa:bb:cc:dd:ee:ff",
            address_type="random",
            name="Longname12",
            manufacturer=None,
            service=None,
            uuid128s=frozenset({"0000180f-0000-1000-8000-00805f9b34fb"}),
            fingerprint_profile="v2",
        )
        self.assertEqual(kind, "fingerprint")
        self.assertTrue(key.startswith("fp:v2:"))


class DecryptWorkflowTests(unittest.TestCase):
    def test_upsert_counts_encrypted_rows(self) -> None:
        conn = _memory_conn()
        sid = "sess-1"
        _seed_session(conn, sid)
        conn.execute(
            """
            INSERT INTO ble_observations(
              session_id, timestamp, address, pdu_type, encrypted_seen
            ) VALUES (?, 10.0, '11:22:33:44:55:66', 'ADV_IND', 1),
                     (?, 11.0, '11:22:33:44:55:66', 'ADV_IND', 0)
            """,
            (sid, sid),
        )
        conn.commit()
        upsert_session_crypto_from_observations(conn, sid)
        row = conn.execute(
            "SELECT encrypted_packets_observed FROM ble_session_crypto WHERE session_id = ?",
            (sid,),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(int(row["encrypted_packets_observed"]), 1)

    def test_set_secrets_path_persists(self) -> None:
        conn = _memory_conn()
        sid = "sess-2"
        _seed_session(conn, sid)
        upsert_session_crypto_from_observations(conn, sid)
        p = Path("C:/tmp/keys.log")
        set_session_secrets_path(conn, sid, p)
        row = conn.execute(
            "SELECT secrets_file_path FROM ble_session_crypto WHERE session_id = ?",
            (sid,),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertIn("keys.log", str(row["secrets_file_path"]))


class AdvancedAnalyticsTests(unittest.TestCase):
    def test_co_presence_graph_metrics(self) -> None:
        g = advanced_analytics.co_presence_graph_metrics(
            [
                {"a": "aa:bb:cc:dd:ee:01", "b": "aa:bb:cc:dd:ee:02", "shared_bins": 3},
                {"a": "aa:bb:cc:dd:ee:01", "b": "aa:bb:cc:dd:ee:03", "shared_bins": 1},
            ],
        )
        self.assertEqual(g["edges"], 2)
        self.assertEqual(g["nodes"], 3)

    def test_composite_narrative_includes_health_flags(self) -> None:
        bullets = advanced_analytics.composite_narrative_hints(
            capture_health_flags=["low_mean_obs_per_active_session"],
            rf_pairs_count=2,
            drift_count=0,
        )
        self.assertTrue(any("Capture-health" in b for b in bullets))


class MultiSensorSqlTests(unittest.TestCase):
    def test_clock_skew_query_runs_on_supported_sqlite(self) -> None:
        conn = _memory_conn()
        _seed_session(conn, "sA", "sn1")
        _seed_session(conn, "sB", "sn2")
        # Same public MAC on two sensors with aligned row indices for the JOIN.
        mac = "10:20:30:40:50:60"
        for i, sid in enumerate(("sA", "sB")):
            for t in (100.0 + i * 0.01, 101.0 + i * 0.01, 102.0 + i * 0.01):
                conn.execute(
                    """
                    INSERT INTO ble_observations(
                      session_id, timestamp, address, address_type, pdu_type, rssi
                    ) VALUES (?, ?, ?, 'public', 'ADV_IND', -50)
                    """,
                    (sid, t, mac),
                )
        conn.commit()
        try:
            out = clock_skew_public_mac_pairs(conn, 99.0, 200.0, limit_pairs=5)
        except sqlite3.OperationalError as e:
            if "syntax error" in str(e).lower() or "window" in str(e).lower():
                self.skipTest(f"SQLite build lacks window functions: {e}")
            raise
        self.assertIsInstance(out, list)


class InsightsReportSmokeTests(unittest.TestCase):
    def test_render_insights_report_minimal_db(self) -> None:
        conn = _memory_conn()
        sid = "sess-smoke"
        _seed_session(conn, sid)
        day = utc_today_iso()
        start, end = utc_day_range_epoch(day, day)
        mid = start + 3600.0
        conn.execute(
            """
            INSERT INTO ble_observations(
              session_id, timestamp, address, address_type, pdu_type, rssi,
              name_hint, manufacturer_hint, encrypted_seen
            ) VALUES (?, ?, 'aa:bb:cc:dd:ee:01', 'public', 'ADV_IND', -55, 'DevSmoke', '0x004C', 0),
                     (?, ?, 'aa:bb:cc:dd:ee:02', 'random', 'ADV_IND', -60, 'OtherDev', '0x0059', 1)
            """,
            (sid, mid, sid, mid + 10.0),
        )
        conn.commit()
        md = render_insights_report(
            conn,
            day,
            day,
            enrich_registry=False,
            materialize_window_stats=False,
        )
        self.assertIn("BLE insights", md)
        self.assertIn("RF-derived inferences", md)
        self.assertIn("signalhub-insights-json", md)


if __name__ == "__main__":
    unittest.main()
