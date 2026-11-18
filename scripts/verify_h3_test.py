"""Behavior checks for the H3 dependency recovery gate."""

import importlib.util
import unittest
from pathlib import Path


source = Path(__file__).with_name("verify-h3.py")
spec = importlib.util.spec_from_file_location("verify_h3", source)
verify_h3 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify_h3)


class AnalyticsRecoveryTest(unittest.TestCase):
    def test_waits_for_temporary_503_then_requires_the_expected_result(self):
        responses = [(503, {"code": "ANALYTICS_TEMPORARILY_UNAVAILABLE"}),
                     (503, {"code": "ANALYTICS_TEMPORARILY_UNAVAILABLE"}),
                     (200, {"data": {"events_accepted": 1}})]
        sleeps = []

        verify_h3.wait_for_recovery("analytics", "/overview", {"Authorization": "Bearer test"},
                                    {"events_accepted": 1},
                                    reader=lambda *_: responses.pop(0), sleeper=sleeps.append)

        self.assertEqual(sleeps, [2, 2])
        self.assertEqual(responses, [])

    def test_stops_after_bounded_unavailability(self):
        calls = []
        sleeps = []

        def unavailable(*_):
            calls.append(1)
            return 503, {"code": "ANALYTICS_TEMPORARILY_UNAVAILABLE"}

        with self.assertRaisesRegex(AssertionError, "did not recover"):
            verify_h3.wait_for_recovery("analytics", "/overview", {}, {},
                                        reader=unavailable, sleeper=sleeps.append, attempts=3)

        self.assertEqual(len(calls), 3)
        self.assertEqual(sleeps, [2, 2])


if __name__ == "__main__":
    unittest.main()
