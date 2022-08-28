import unittest
from os import path

from beancount.ingest import regression_pytest as regtest
from clerk_importer.importer import Importer

IMPORTER = Importer(clerk_db=".clerk/data/clerk.db")

@regtest.with_importer(IMPORTER)
@regtest.with_testdir(path.dirname(__file__))
class TestImporter(regtest.ImporterTestBase):
    pass

if __name__ == '__main__':
    unittest.main()
