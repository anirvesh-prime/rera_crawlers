from __future__ import annotations

import os
import unittest

from core.crawler_base import generate_project_key


class ProjectKeyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_seed = os.environ.get("PYTHONHASHSEED")
        os.environ["PYTHONHASHSEED"] = "0"

    def tearDown(self) -> None:
        if self.original_seed is None:
            os.environ.pop("PYTHONHASHSEED", None)
        else:
            os.environ["PYTHONHASHSEED"] = self.original_seed

    def test_key_matches_existing_legacy_value(self):
        self.assertEqual(generate_project_key("K-RERA/PRJ/KKD/051/2026"), "17186159861670388125")

    def test_key_is_stable_for_same_registration_number(self):
        first = generate_project_key("K-RERA/PRJ/KKD/051/2026")
        second = generate_project_key("K-RERA/PRJ/KKD/051/2026")
        self.assertEqual(first, second)

    def test_key_strips_surrounding_whitespace(self):
        clean = generate_project_key("K-RERA/PRJ/KKD/051/2026")
        padded = generate_project_key("  K-RERA/PRJ/KKD/051/2026  ")
        self.assertEqual(clean, padded)


if __name__ == "__main__":
    unittest.main()
