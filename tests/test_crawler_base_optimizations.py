from __future__ import annotations

import unittest
from unittest.mock import patch

from core.config import settings
from core.crawler_base import get_scaled_delay_range, random_delay


class CrawlerBaseOptimizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_delay_scale = settings.CRAWL_DELAY_SCALE

    def tearDown(self) -> None:
        settings.CRAWL_DELAY_SCALE = self.original_delay_scale

    def test_get_scaled_delay_range_applies_global_scale(self):
        settings.CRAWL_DELAY_SCALE = 0.25
        self.assertEqual(get_scaled_delay_range(2.0, 4.0), (0.5, 1.0))

    @patch("core.crawler_base.time.sleep")
    @patch("core.crawler_base.random.uniform", return_value=0.75)
    def test_random_delay_uses_scaled_range(self, mock_uniform, mock_sleep):
        settings.CRAWL_DELAY_SCALE = 0.25

        random_delay(2.0, 4.0)

        mock_uniform.assert_called_once_with(0.5, 1.0)
        mock_sleep.assert_called_once_with(0.75)

    @patch("core.crawler_base.time.sleep")
    @patch("core.crawler_base.random.uniform")
    def test_random_delay_skips_sleep_when_scale_is_zero(self, mock_uniform, mock_sleep):
        settings.CRAWL_DELAY_SCALE = 0.0

        random_delay(2.0, 4.0)

        mock_uniform.assert_not_called()
        mock_sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
