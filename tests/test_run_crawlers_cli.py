from __future__ import annotations

import argparse
import contextlib
import io
import os
import sys
import unittest
from unittest.mock import patch

from core.config import settings
from run_crawlers import apply_runtime_overrides, main, parse_args


class RunCrawlersCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_env = os.environ.get("CRAWL_ITEM_LIMIT")
        self.original_limit = settings.CRAWL_ITEM_LIMIT
        self.original_delay_env = os.environ.get("CRAWL_DELAY_SCALE")
        self.original_delay_scale = settings.CRAWL_DELAY_SCALE
        self.original_target_env = os.environ.get("TARGET_REG_NO")
        self.original_target = settings.TARGET_REG_NO
        self.original_skip_docs_env = os.environ.get("SKIP_DOCUMENTS")
        self.original_skip_docs = settings.SKIP_DOCUMENTS
        self.original_skip_light_new_env = os.environ.get("LIGHT_SKIP_NEW_ADDITIONS")
        self.original_skip_light_new = settings.LIGHT_SKIP_NEW_ADDITIONS
        self.original_test_mode_env = os.environ.get("TEST_MODE")
        self.original_test_mode = settings.TEST_MODE
        self.original_dry_run_s3_env = os.environ.get("DRY_RUN_S3")
        self.original_dry_run_s3 = settings.DRY_RUN_S3
        self.original_crawler_tester_env = os.environ.get("CRAWLER_TESTER")
        self.original_crawler_tester = settings.CRAWLER_TESTER

    def tearDown(self) -> None:
        if self.original_env is None:
            os.environ.pop("CRAWL_ITEM_LIMIT", None)
        else:
            os.environ["CRAWL_ITEM_LIMIT"] = self.original_env
        settings.CRAWL_ITEM_LIMIT = self.original_limit
        if self.original_delay_env is None:
            os.environ.pop("CRAWL_DELAY_SCALE", None)
        else:
            os.environ["CRAWL_DELAY_SCALE"] = self.original_delay_env
        settings.CRAWL_DELAY_SCALE = self.original_delay_scale
        if self.original_target_env is None:
            os.environ.pop("TARGET_REG_NO", None)
        else:
            os.environ["TARGET_REG_NO"] = self.original_target_env
        settings.TARGET_REG_NO = self.original_target
        if self.original_skip_docs_env is None:
            os.environ.pop("SKIP_DOCUMENTS", None)
        else:
            os.environ["SKIP_DOCUMENTS"] = self.original_skip_docs_env
        settings.SKIP_DOCUMENTS = self.original_skip_docs
        if self.original_skip_light_new_env is None:
            os.environ.pop("LIGHT_SKIP_NEW_ADDITIONS", None)
        else:
            os.environ["LIGHT_SKIP_NEW_ADDITIONS"] = self.original_skip_light_new_env
        settings.LIGHT_SKIP_NEW_ADDITIONS = self.original_skip_light_new
        if self.original_test_mode_env is None:
            os.environ.pop("TEST_MODE", None)
        else:
            os.environ["TEST_MODE"] = self.original_test_mode_env
        settings.TEST_MODE = self.original_test_mode
        if self.original_dry_run_s3_env is None:
            os.environ.pop("DRY_RUN_S3", None)
        else:
            os.environ["DRY_RUN_S3"] = self.original_dry_run_s3_env
        settings.DRY_RUN_S3 = self.original_dry_run_s3
        if self.original_crawler_tester_env is None:
            os.environ.pop("CRAWLER_TESTER", None)
        else:
            os.environ["CRAWLER_TESTER"] = self.original_crawler_tester_env
        settings.CRAWLER_TESTER = self.original_crawler_tester

    def test_apply_runtime_overrides_sets_item_limit(self):
        args = argparse.Namespace(item_limit=7, no_item_limit=False, delay_scale=None)
        result = apply_runtime_overrides(args)
        self.assertEqual(result, 7)
        self.assertEqual(settings.CRAWL_ITEM_LIMIT, 7)
        self.assertEqual(os.environ["CRAWL_ITEM_LIMIT"], "7")

    def test_apply_runtime_overrides_clears_item_limit(self):
        os.environ["CRAWL_ITEM_LIMIT"] = "11"
        settings.CRAWL_ITEM_LIMIT = 11
        args = argparse.Namespace(item_limit=None, no_item_limit=True, delay_scale=None)
        result = apply_runtime_overrides(args)
        self.assertEqual(result, 0)
        self.assertEqual(settings.CRAWL_ITEM_LIMIT, 0)
        self.assertNotIn("CRAWL_ITEM_LIMIT", os.environ)

    def test_apply_runtime_overrides_sets_delay_scale(self):
        args = argparse.Namespace(item_limit=None, no_item_limit=False, delay_scale=0.5)
        result = apply_runtime_overrides(args)
        self.assertEqual(result, self.original_limit)
        self.assertEqual(settings.CRAWL_DELAY_SCALE, 0.5)
        self.assertEqual(os.environ["CRAWL_DELAY_SCALE"], "0.5")

    def test_apply_runtime_overrides_sets_target_reg_no(self):
        args = argparse.Namespace(
            item_limit=None, no_item_limit=False, delay_scale=None,
            target_reg_no="  PRM/KA/RERA/1251/446/PR/181122/005482  ",
        )
        apply_runtime_overrides(args)
        self.assertEqual(settings.TARGET_REG_NO,
                         "PRM/KA/RERA/1251/446/PR/181122/005482")
        self.assertEqual(os.environ["TARGET_REG_NO"],
                         "PRM/KA/RERA/1251/446/PR/181122/005482")

    def test_apply_runtime_overrides_target_reg_no_default_unset(self):
        args = argparse.Namespace(
            item_limit=None, no_item_limit=False, delay_scale=None,
        )
        apply_runtime_overrides(args)
        self.assertEqual(settings.TARGET_REG_NO, self.original_target)

    def test_apply_runtime_overrides_sets_skip_documents(self):
        args = argparse.Namespace(
            item_limit=None, no_item_limit=False, delay_scale=None,
            skip_documents=True,
        )
        apply_runtime_overrides(args)
        self.assertTrue(settings.SKIP_DOCUMENTS)
        self.assertEqual(os.environ["SKIP_DOCUMENTS"], "true")

    def test_parse_args_accepts_short_skip_new_flag(self):
        with patch("sys.argv", ["run_crawlers.py", "--skip-new"]):
            args = parse_args()
        self.assertTrue(args.skip_light_new_additions)

    def test_parse_args_accepts_legacy_skip_light_new_additions_flag(self):
        with patch("sys.argv", ["run_crawlers.py", "--skip-light-new-additions"]):
            args = parse_args()
        self.assertTrue(args.skip_light_new_additions)

    def test_apply_runtime_overrides_sets_skip_light_new_additions(self):
        args = argparse.Namespace(
            item_limit=None, no_item_limit=False, delay_scale=None,
            skip_light_new_additions=True,
        )
        apply_runtime_overrides(args)
        self.assertTrue(settings.LIGHT_SKIP_NEW_ADDITIONS)
        self.assertEqual(os.environ["LIGHT_SKIP_NEW_ADDITIONS"], "true")

    def test_tester_requires_exactly_one_site_returns_error(self):
        selected_sites = [{"id": "kerala_rera"}, {"id": "rajasthan_rera"}]
        argv = [
            "run_crawlers.py",
            "--mode",
            "daily_light",
            "--skip-documents",
            "--tester",
        ]
        with patch.object(sys, "argv", argv), patch(
            "run_crawlers.select_sites",
            return_value=(selected_sites, [], []),
        ), contextlib.redirect_stdout(io.StringIO()) as output:
            exit_code = main()

        self.assertEqual(exit_code, 2)
        self.assertIn("--tester requires exactly one --site", output.getvalue())


if __name__ == "__main__":
    unittest.main()
