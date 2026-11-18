"""Behavior checks for the J3 evidence collector."""

import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from benchmark import (ensure_clean, evaluate, observe_drain, parse_kafka_lag,
                       parse_kafka_positions, parse_metrics, timed_probe,
                       pipeline_drained, snapshot, summarize_records)
from run_diagnostics import diagnostic_plan, diagnostics_failed
from aggregate_producers import aggregate_from_plan, aggregate_instances


class BenchmarkEvidenceTest(unittest.TestCase):
    def test_distributed_producer_plan_reads_sanitized_shard_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            start = "2026-11-19T10:00:00.000Z"
            plan = {"version": 1, "run_id": "normal-a", "start_at_utc": start,
                    "expected_rate": 1, "duration_seconds": 1, "payload_bytes": 1024,
                    "instances": [{"instance_id": "a", "summary": "a.json",
                                   "records": "a.ndjson"}]}
            summary = {"run_id": "normal-a", "instance_id": "a", "requested_rate": 1,
                       "requested_duration_seconds": 1, "payload_bytes": 1024,
                       "planned_start_at_utc": start, "started_at_utc": start,
                       "finished_at_utc": "2026-11-19T10:00:01.000Z",
                       "scheduled": 1, "completed": 1, "accepted": 1, "rejected": 0,
                       "errors": 0, "skipped": 0}
            record = {"sequence": 0, "tenant_index": 0, "status": 202,
                      "outcome": "accepted", "sent_offset_ms": 0,
                      "completed_offset_ms": 5, "latency_ms": 5}
            (root / "plan.json").write_text(json.dumps(plan))
            (root / "a.json").write_text(json.dumps(summary))
            (root / "a.ndjson").write_text(json.dumps(record) + "\n")
            self.assertEqual(aggregate_from_plan(root / "plan.json")["summary"]["accepted"], 1)
            summary["accepted"] = 0
            (root / "a.json").write_text(json.dumps(summary))
            with self.assertRaisesRegex(ValueError, "summary disagrees"):
                aggregate_from_plan(root / "plan.json")

    def test_distributed_producer_aggregation_checks_each_shard_and_start_skew(self):
        plan = {"version": 1, "run_id": "normal-a", "start_at_utc": "2026-11-19T10:00:00.000Z",
                "expected_rate": 4, "duration_seconds": 1, "payload_bytes": 1024,
                "instances": [{"instance_id": "a"}, {"instance_id": "b"}]}
        def shard(instance, started, outcomes):
            records = [{"sequence": index, "tenant_index": 0,
                        "status": 202 if outcome == "accepted" else 503,
                        "outcome": outcome, "sent_offset_ms": index * 200,
                        "completed_offset_ms": index * 200 + 10,
                        "latency_ms": 10} for index, outcome in enumerate(outcomes)]
            summary = {"run_id": "normal-a", "instance_id": instance,
                       "requested_rate": 2, "requested_duration_seconds": 1,
                       "payload_bytes": 1024, "planned_start_at_utc": plan["start_at_utc"],
                       "started_at_utc": started, "finished_at_utc": "2026-11-19T10:00:01.000Z",
                       "scheduled": 2, "completed": 2,
                       "accepted": outcomes.count("accepted"),
                       "rejected": outcomes.count("rejected"), "errors": 0, "skipped": 0}
            return {"summary": summary, "records": records}
        runs = {"a": shard("a", "2026-11-19T10:00:00.100Z", ["accepted", "accepted"]),
                "b": shard("b", "2026-11-19T10:00:00.400Z", ["accepted", "rejected"])}
        result = aggregate_instances(plan, runs)
        self.assertEqual(result["summary"]["accepted"], 3)
        self.assertEqual(result["summary"]["accepted_within_window"], 3)
        self.assertEqual(result["summary"]["rejections"], {"503": 1})
        self.assertEqual(result["summary"]["accepted_latency_ms"]["p95"], 10)
        self.assertEqual(result["start_skew_ms"], 300)
        runs["b"]["summary"]["started_at_utc"] = "2026-11-19T10:00:02.000Z"
        with self.assertRaisesRegex(ValueError, "start"):
            aggregate_instances(plan, runs)
        runs["b"]["summary"]["started_at_utc"] = "2026-11-19T10:00:00.400Z"
        runs["b"]["records"][1]["sequence"] = 0
        with self.assertRaisesRegex(ValueError, "sequence"):
            aggregate_instances(plan, runs)
        runs["b"]["records"][1]["sequence"] = 1
        runs["a"]["records"][1]["sent_offset_ms"] = 1200
        runs["a"]["records"][1]["completed_offset_ms"] = 1210
        late = aggregate_instances(plan, runs)
        self.assertEqual(late["summary"]["sent_after_window"], 1)
        self.assertEqual(late["summary"]["accepted_within_window"], 2)

    def test_progress_snapshot_reads_only_drain_state(self):
        kafka = ("GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n"
                 "analytics relayforge.analytics.delivery-facts.v1 0 7 7 0 id host client\n")
        ingestion = '{"events":1,"outbox_pending":0}'
        delivery = ('{"routing_events":1,"routing_incomplete":0,"deliveries":3,'
                    '"succeeded":3,"dead_lettered":0,"backlog":0,"outbox_pending":0,'
                    '"outbox_by_type":{"delivery.ready.v1":{"pending":0,"published":3},'
                    '"analytics.delivery-facts.v1":{"pending":0,"published":9}}}')
        with patch("benchmark.command", return_value=kafka) as commands, \
                patch("benchmark.sql", side_effect=[ingestion, delivery]) as queries:
            result = snapshot(["docker", "compose"], {}, detail=False)
        self.assertTrue(pipeline_drained(result))
        self.assertEqual(commands.call_count, 1)
        self.assertEqual(queries.call_count, 2)
        self.assertNotIn("wal_bytes", queries.call_args_list[0].args[3])
        self.assertIn("GROUP BY message_type", queries.call_args_list[1].args[3])
        self.assertEqual(result["delivery"]["outbox_by_type"]["analytics.delivery-facts.v1"]
                         ["published"], 9)
        self.assertNotIn("delivery_pool", result)
        self.assertEqual(set(result["probe_duration_seconds"]),
                         {"kafka", "ingestion", "delivery"})

    def test_timed_probe_records_duration_and_keeps_probe_error(self):
        durations = {}
        clock = iter([10.0, 10.25, 11.0, 11.75])
        self.assertEqual(timed_probe("healthy", lambda: {"value": 1}, durations,
                                     lambda: next(clock)), {"value": 1})
        self.assertEqual(timed_probe("failed", lambda: (_ for _ in ()).throw(ValueError()),
                                     durations, lambda: next(clock)),
                         {"probe_error": "ValueError"})
        self.assertEqual(durations, {"healthy": 0.25, "failed": 0.75})

    def test_kafka_positions_sum_partitions_for_stage_throughput(self):
        output = """GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
analytics relayforge.analytics.delivery-facts.v1 0 12 20 8 id host client
analytics relayforge.analytics.delivery-facts.v1 1 7 10 3 id host client
analytics relayforge.analytics.delivery-facts.v1 2 - 4 - id host client
"""
        self.assertEqual(parse_kafka_positions(output), {
            "relayforge.analytics.delivery-facts.v1": {
                "committed": 19, "end": 30, "lag": 11},
        })

    def test_metric_probe_keeps_named_gauges_and_timer_totals(self):
        names = ["hikaricp.connections.active", "relayforge.delivery.handler.duration"]
        output = ('{"measurements":[{"statistic":"VALUE","value":7}]}\n'
                  '{"measurements":[{"statistic":"COUNT","value":30},'
                  '{"statistic":"TOTAL_TIME","value":8.5}]}\n')
        self.assertEqual(parse_metrics(names, output), {
            names[0]: {"VALUE": 7},
            names[1]: {"COUNT": 30, "TOTAL_TIME": 8.5},
        })

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

    def test_controlled_plan_runs_two_independent_low_rate_windows(self):
        plan = diagnostic_plan("j3-delivery", controlled=True)
        self.assertEqual([(item["profile"], item["duration_seconds"]) for item in plan],
                         [("controlled", 20), ("controlled", 20)])
        self.assertEqual(len({item["project"] for item in plan}), 2)

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

    def test_drain_detected_by_a_snapshot_crossing_deadline_is_recorded(self):
        waiting = {"ingestion": {"events": 1, "outbox_pending": 0},
                   "delivery": {"routing_events": 1, "routing_incomplete": 0,
                                "deliveries": 1, "succeeded": 0, "dead_lettered": 0,
                                "backlog": 1, "outbox_pending": 0},
                   "kafka_lag": {"relayforge.delivery.ready.v1": 0}}
        done = {"ingestion": {"events": 1, "outbox_pending": 0},
                "delivery": {"routing_events": 1, "routing_incomplete": 0,
                             "deliveries": 1, "succeeded": 1, "dead_lettered": 0,
                             "backlog": 0, "outbox_pending": 0},
                "kafka_lag": {"relayforge.delivery.ready.v1": 0}}
        elapsed = [0]

        def sample():
            elapsed[0] += 6
            return done

        def sleep(seconds):
            elapsed[0] += seconds

        drain_seconds, samples, final = observe_drain(
            waiting, sample, 10, clock=lambda: elapsed[0], sleeper=sleep)
        self.assertEqual(drain_seconds, 11)
        self.assertEqual(samples, [done])
        self.assertIs(final, done)


if __name__ == "__main__":
    unittest.main()
