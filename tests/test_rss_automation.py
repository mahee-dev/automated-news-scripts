import importlib.util
import os
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_script_module(filename, module_name):
    os.environ["DATABASE_URL"] = "sqlite:///:memory:"
    os.environ["AI_PROVIDER"] = "openrouter"
    os.environ["OPENROUTER_API_KEY"] = "test-key"

    spec = importlib.util.spec_from_file_location(module_name, ROOT / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetcher = load_script_module("rss-fetcher-v10.py", "rss_fetcher_v10")
analyser = load_script_module("rss-analyser-v10.py", "rss_analyser_v10")


class FetcherEntryPreparationTests(unittest.TestCase):
    def test_uses_updated_date_when_published_date_is_missing(self):
        entry = {
            "title": "Fallback date",
            "link": "https://example.com/fallback",
            "updated_parsed": time.struct_time((2026, 7, 2, 9, 30, 0, 3, 183, 0)),
        }

        prepared = fetcher.prepare_feed_entry(entry)

        self.assertIsNotNone(prepared)
        self.assertEqual(prepared[1].isoformat(), "2026-07-02T09:30:00")

    def test_skips_entries_without_usable_date_or_link(self):
        missing_date = {"title": "No date", "link": "https://example.com/no-date"}
        missing_link = {
            "title": "No link",
            "published_parsed": time.struct_time((2026, 7, 2, 9, 30, 0, 3, 183, 0)),
        }

        self.assertIsNone(fetcher.prepare_feed_entry(missing_date))
        self.assertIsNone(fetcher.prepare_feed_entry(missing_link))


class AnalyserValidationTests(unittest.TestCase):
    def test_normalizes_valid_ai_response(self):
        result = analyser.normalize_article_response({
            "translated_title": "Title",
            "translated_description": "Description",
            "keywords": [" economy ", 2026, ""],
            "sentiment": " Positive ",
            "category": "politics",
        })

        self.assertEqual(result.sentiment, "positive")
        self.assertEqual(result.category, "Politics")
        self.assertEqual(result.keywords, ["economy", "2026"])

    def test_rejects_invalid_sentiment(self):
        with self.assertRaises(ValueError):
            analyser.normalize_article_response({
                "translated_title": "Title",
                "translated_description": "Description",
                "keywords": ["economy"],
                "sentiment": "mixed",
                "category": "Politics",
            })

    def test_rejects_invalid_category(self):
        with self.assertRaises(ValueError):
            analyser.normalize_article_response({
                "translated_title": "Title",
                "translated_description": "Description",
                "keywords": ["economy"],
                "sentiment": "neutral",
                "category": "Finance",
            })

    def test_rejects_malformed_keywords(self):
        with self.assertRaises(ValueError):
            analyser.normalize_article_response({
                "translated_title": "Title",
                "translated_description": "Description",
                "keywords": {"topic": "economy"},
                "sentiment": "neutral",
                "category": "Business",
            })


class MarkProcessedTests(unittest.TestCase):
    def test_marks_only_successful_ids(self):
        original_execute_batch = analyser.execute_batch
        captured = {}

        def fake_execute_batch(cursor, statement, params):
            captured["statement"] = statement
            captured["params"] = params

        class FakeCursor:
            def __init__(self):
                self.statements = []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, traceback):
                return False

            def execute(self, statement):
                self.statements.append(statement)

        class FakeConnection:
            def __init__(self):
                self.cursor_instance = FakeCursor()

            def cursor(self):
                return self.cursor_instance

        try:
            analyser.execute_batch = fake_execute_batch
            analyser.mark_as_processed(FakeConnection(), [2, 5])
        finally:
            analyser.execute_batch = original_execute_batch

        self.assertEqual(captured["params"], [(2,), (5,)])
        self.assertIn("processed = TRUE", captured["statement"])


if __name__ == "__main__":
    unittest.main()
