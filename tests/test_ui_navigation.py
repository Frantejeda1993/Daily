import unittest

from ui_navigation import build_tab_labels


class TestUINavigation(unittest.TestCase):
    def test_build_tab_labels_includes_dynamic_groups(self):
        groups = ["2 Wheels", "Free Time", "Outdoor Tech", "New Group"]

        labels = build_tab_labels(groups)

        self.assertEqual(
            labels,
            [
                "Margenes",
                "Recap",
                "2 Wheels",
                "Free Time",
                "Outdoor Tech",
                "New Group",
                "Configuracion",
                "Actualizacion",
            ],
        )


if __name__ == "__main__":
    unittest.main()
