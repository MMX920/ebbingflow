import json
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from dateutil.relativedelta import relativedelta

# Add project root to path
sys.path.append(os.getcwd())

from memory.graph.writer import AsyncGraphWriter


class MemoryEventMock:
    def __init__(self, timestamp_reference=None, event_time=None):
        self.timestamp_reference = timestamp_reference
        self.event_time = event_time


class TestTemporalGoldens(unittest.TestCase):
    def setUp(self):
        with patch("memory.graph.writer.AsyncGraphDatabase"):
            self.writer = AsyncGraphWriter()
        self.now = datetime.now(timezone.utc).replace(tzinfo=None)

    def test_temporal_hit_rate(self):
        # 30-case golden set: relative zh + absolute dates/timestamps.
        goldens = [
            # relative day-level (10)
            {"input": "\u4eca\u5929", "expected": self.now.date().isoformat()},
            {"input": "\u6628\u5929", "expected": (self.now - timedelta(days=1)).date().isoformat()},
            {"input": "\u524d\u5929", "expected": (self.now - timedelta(days=2)).date().isoformat()},
            {"input": "\u660e\u5929", "expected": (self.now + timedelta(days=1)).date().isoformat()},
            {"input": "\u73b0\u5728", "expected": self.now.date().isoformat()},
            {"input": "3 days ago", "expected": (self.now - timedelta(days=3)).date().isoformat()},
            {"input": "5 days ago", "expected": (self.now - timedelta(days=5)).date().isoformat()},
            {"input": "2 days later", "expected": (self.now + timedelta(days=2)).date().isoformat()},
            {"input": "in 10 days", "expected": (self.now + timedelta(days=10)).date().isoformat()},
            {"input": "1 day ago", "expected": (self.now - timedelta(days=1)).date().isoformat()},

            # relative week/month/year (10)
            {"input": "\u4e09\u5929\u524d", "expected": (self.now - timedelta(days=3)).date().isoformat()},
            {"input": "\u4e94\u5929\u540e", "expected": (self.now + timedelta(days=5)).date().isoformat()},
            {"input": "\u4e00\u5468\u524d", "expected": (self.now - timedelta(days=7)).date().isoformat()},
            {"input": "\u4e24\u5468\u540e", "expected": (self.now + timedelta(days=14)).date().isoformat()},
            {"input": "\u4e09\u5468\u540e", "expected": (self.now + timedelta(days=21)).date().isoformat()},
            {"input": "\u4e00\u4e2a\u6708\u524d", "expected": (self.now - relativedelta(months=1)).date().isoformat()},
            {"input": "\u4e24\u4e2a\u6708\u524d", "expected": (self.now - relativedelta(months=2)).date().isoformat()},
            {"input": "\u4e00\u5e74\u524d", "expected": (self.now - relativedelta(years=1)).date().isoformat()},
            {"input": "\u4e24\u5e74\u524d", "expected": (self.now - relativedelta(years=2)).date().isoformat()},
            {"input": "\u53bb\u5e74\u4eca\u5929", "expected": (self.now - relativedelta(years=1)).date().isoformat()},

            # absolute date/time (10)
            {"input": "2023-10-27", "expected": "2023-10-27"},
            {"input": "2023/10/27", "expected": "2023-10-27"},
            {"input": "2024-05-20", "expected": "2024-05-20"},
            {"input": "1999/12/31", "expected": "1999-12-31"},
            {"input": "2025-01-01T12:00:00", "expected": "2025-01-01T12:00:00"},
            {"input": "2022-02-02", "expected": "2022-02-02"},
            {"input": "2021/11/11", "expected": "2021-11-11"},
            {"input": "2020-05-05", "expected": "2020-05-05"},
            {"input": "2019/09/09", "expected": "2019-09-09"},
            {"input": "2008-08-08", "expected": "2008-08-08"},
        ]

        hits = 0
        results = []
        for case in goldens:
            event = MemoryEventMock(timestamp_reference=case["input"])
            actual = self.writer._normalize_event_time(event)
            is_hit = actual == case["expected"]
            if is_hit:
                hits += 1
            results.append(
                {
                    "input": case["input"],
                    "expected": case["expected"],
                    "actual": actual,
                    "hit": is_hit,
                }
            )

        hit_rate = hits / len(goldens)
        report = {
            "total": len(goldens),
            "hits": hits,
            "hit_rate": f"{hit_rate:.2%}",
            "details": results,
        }

        os.makedirs(".data", exist_ok=True)
        with open(".data/temporal_hit_rate.json", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        print(f"Temporal Hit Rate: {hit_rate:.2%}")
        self.assertGreaterEqual(hit_rate, 0.9, f"Hit rate {hit_rate:.2%} is below threshold 90%")


if __name__ == "__main__":
    unittest.main()
