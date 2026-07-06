from __future__ import annotations

import unittest
from unittest import mock

from core import db
from core.config import settings


class DbTesterModeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_test_mode = settings.TEST_MODE
        self.original_crawler_tester = settings.CRAWLER_TESTER

    def tearDown(self) -> None:
        settings.TEST_MODE = self.original_test_mode
        settings.CRAWLER_TESTER = self.original_crawler_tester

    def test_upsert_project_does_not_connect_in_crawler_tester(self):
        settings.TEST_MODE = True
        settings.CRAWLER_TESTER = True

        with mock.patch.object(db, "get_connection") as get_connection:
            result = db.upsert_project({
                "key": "tester-key",
                "project_registration_no": "TEST/001",
            })

        self.assertEqual(result, "new")
        get_connection.assert_not_called()


if __name__ == "__main__":
    unittest.main()
