from __future__ import annotations

import os
import unittest

from core.crawler_base import generate_project_key


class BiharProjectKeyTests(unittest.TestCase):
    """Bihar prod key = siphash24(project_name + reg_no + promoter_name, seed=0).

    Values are concatenated raw (no separator, no case/whitespace changes), matching
    what the prod pipeline historically produced. Verified against a live prod row.
    """

    def setUp(self) -> None:
        self.original_seed = os.environ.get("PYTHONHASHSEED")
        os.environ["PYTHONHASHSEED"] = "0"

    def tearDown(self) -> None:
        if self.original_seed is None:
            os.environ.pop("PYTHONHASHSEED", None)
        else:
            os.environ["PYTHONHASHSEED"] = self.original_seed

    def test_bihar_key_matches_prod_observed_value(self):
        project_name  = "The Residency-City Centre Patna"
        reg_no        = "BRERAP05734-1/994/R-766/2019"
        promoter_name = "UTKARSH SFATIK LIMITED"
        self.assertEqual(
            generate_project_key(project_name + reg_no + promoter_name),
            "2176179351295726697",
        )

    def test_bihar_key_differs_from_legacy_reg_only_key(self):
        reg_no        = "BRERAP05734-1/994/R-766/2019"
        project_name  = "The Residency-City Centre Patna"
        promoter_name = "UTKARSH SFATIK LIMITED"
        self.assertNotEqual(
            generate_project_key(reg_no),
            generate_project_key(project_name + reg_no + promoter_name),
        )


if __name__ == "__main__":
    unittest.main()
