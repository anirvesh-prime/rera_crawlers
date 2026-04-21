from __future__ import annotations

import argparse
import os
import unittest

from core.config import settings
from run_crawlers import apply_runtime_overrides


class RunCrawlersCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_env = os.environ.get("CRAWL_ITEM_LIMIT")
        self.original_limit = settings.CRAWL_ITEM_LIMIT

    def tearDown(self) -> None:
        if self.original_env is None:
            os.environ.pop("CRAWL_ITEM_LIMIT", None)
        else:
            os.environ["CRAWL_ITEM_LIMIT"] = self.original_env
        settings.CRAWL_ITEM_LIMIT = self.original_limit

    def test_apply_runtime_overrides_sets_item_limit(self):
        args = argparse.Namespace(item_limit=7, no_item_limit=False)
        result = apply_runtime_overrides(args)
        self.assertEqual(result, 7)
        self.assertEqual(settings.CRAWL_ITEM_LIMIT, 7)
        self.assertEqual(os.environ["CRAWL_ITEM_LIMIT"], "7")

    def test_apply_runtime_overrides_clears_item_limit(self):
        os.environ["CRAWL_ITEM_LIMIT"] = "11"
        settings.CRAWL_ITEM_LIMIT = 11
        args = argparse.Namespace(item_limit=None, no_item_limit=True)
        result = apply_runtime_overrides(args)
        self.assertEqual(result, 0)
        self.assertEqual(settings.CRAWL_ITEM_LIMIT, 0)
        self.assertNotIn("CRAWL_ITEM_LIMIT", os.environ)


if __name__ == "__main__":
    unittest.main()
