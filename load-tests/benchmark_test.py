"""Behavior checks for the J3 evidence collector."""

import unittest

from benchmark import ensure_clean, evaluate, parse_kafka_lag, pipeline_drained, summarize_records
from run_diagnostics import diagnostic_plan, diagnostics_failed


class BenchmarkEvidenceTest(unittest.TestCase):
    def test_kafka_lag_is_summed_by_topic_without_consumer_identity(self):
        output = """GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
router relayforge.event.accepted.v1 0 10 16 6 id host client
router relayforge.event.accepted.v1 1 20 23 3 id host client
worker relayforge.delivery.ready.v1 0 5 12 7 id host client
"""
        self.assertEqual(parse_kafka_lag(output), {
            "relayforge.event.accepted.v1": 9,
            "relayforge.delivery.ready.v1": 7,
        })

    def test_summary_keeps_rejections_and_skips_distinct_from_acceptance(self):
        records = [
            {"outcome": "accepted", "latency_ms": 10},
            {"outcome": "accepted", "latency_ms": 30},
            {"outcome": "rejected", "status": 429, "latency_ms": 50},
            {"outcome": "error", "error_code": "ECONNRESET", "latency_ms": 70},
        ]
        self.assertEqual(summarize_records(records, scheduled=5)["accepted_latency_ms"]["p95"], 30)
        self.assertEqual(summarize_records(records, scheduled=5)["rejections"], {"429": 1})
        self.assertEqual(summarize_records(records, scheduled=5)["errors"], {"ECONNRESET": 1})
        self.assertEqual(summarize_records(records, scheduled=5)["skipped"], 1)

    def test_unmet_profile_is_a_finding_even_when_latency_is_low(self):
        result = evaluate({"scheduled": 100, "accepted": 98, "skipped": 0,
                           "accepted_latency_ms": {"p95": 10, "p99": 20}},
                          target_rate=100, duration_seconds=1)
        self.assertEqual(result["status"], "unmet")
        self.assertIn("accepted_rate", result["findings"])

    def test_dirty_start_is_rejected_before_a_new_profile(self):
        snapshot = {"ingestion": {"outbox_pending": 0},
                    "delivery": {"backlog": 3, "outbox_pending": 0},
                    "kafka_lag": {"relayforge.event.accepted.v1": 0}}
        self.assertIn("delivery backlog", ensure_clean(snapshot))

    def test_diagnostic_plan_uses_fresh_projects_and_bounded_windows(self):
        plan = diagnostic_plan("j3-check")
        self.assertEqual(len(plan), 6)
        self.assertEqual(len({item["project"] for item in plan}), 6)
        self.assertEqual([(item["profile"], item["duration_seconds"]) for item in plan],
                         [("normal", 5), ("normal", 5), ("peak", 3), ("peak", 3),
                          ("headroom", 2), ("headroom", 2)])

    def test_diagnostic_runner_fails_when_a_run_crashes(self):
        self.assertFalse(diagnostics_failed([{"status": "unmet"}]))
        self.assertTrue(diagnostics_failed([{"status": "unmet"},
                                            {"error_type": "TimeoutExpired"}]))

    def test_drain_requires_every_accepted_event_to_route(self):
        snapshot = {"ingestion": {"events": 10, "outbox_pending": 0},
                    "delivery": {"routing_events": 9, "routing_incomplete": 0,
                                 "deliveries": 30, "succeeded": 30, "dead_lettered": 0,
                                 "backlog": 0, "outbox_pending": 0},
                    "kafka_lag": {"relayforge.event.accepted.v1": 0}}
        self.assertFalse(pipeline_drained(snapshot))
        snapshot["delivery"]["routing_events"] = 10
        self.assertTrue(pipeline_drained(snapshot))

    def test_drain_requires_routing_and_delivery_to_finish(self):
        snapshot = {"ingestion": {"events": 1, "outbox_pending": 0},
                    "delivery": {"routing_events": 1, "routing_incomplete": 1,
                                 "deliveries": 3, "succeeded": 2, "dead_lettered": 0,
                                 "backlog": 0, "outbox_pending": 0},
                    "kafka_lag": {"relayforge.event.accepted.v1": 0}}
        self.assertFalse(pipeline_drained(snapshot))
        snapshot["delivery"]["routing_incomplete"] = 0
        self.assertFalse(pipeline_drained(snapshot))
        snapshot["delivery"]["succeeded"] = 3
        self.assertTrue(pipeline_drained(snapshot))


if __name__ == "__main__":
    unittest.main()
