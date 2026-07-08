import unittest


class SmokeTest(unittest.TestCase):
    def test_imports(self):
        import config  # noqa: F401
        import extract.coingecko  # noqa: F401
        import transform.indicators  # noqa: F401
        import load.postgres  # noqa: F401


if __name__ == "__main__":
    unittest.main()
