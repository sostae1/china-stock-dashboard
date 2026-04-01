import unittest

from plugins.data_collection.utils.provider_preference import normalize_provider_preference, reorder_provider_chain


class TestProviderPreference(unittest.TestCase):
    def test_aliases(self):
        self.assertEqual(normalize_provider_preference("em"), "eastmoney")
        self.assertEqual(normalize_provider_preference("dongcai"), "eastmoney")
        self.assertEqual(normalize_provider_preference("auto"), "auto")

    def test_reorder(self):
        chain = [("a", 1), ("b", 2), ("a", 3)]
        out = reorder_provider_chain("a", chain)
        self.assertEqual(out, [("a", 1), ("a", 3), ("b", 2)])

    def test_auto_unchanged(self):
        chain = [("a", 1), ("b", 2)]
        self.assertEqual(reorder_provider_chain("auto", chain), chain)


if __name__ == "__main__":
    unittest.main()
