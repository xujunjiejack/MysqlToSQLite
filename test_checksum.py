import unittest
from migrator_kit.checksumTable import ChecksumTable
import pyodbc


class MyTestCase(unittest.TestCase):
    def test_checksum(self):
        checksumTable = ChecksumTable()
        con = pyodbc.connect("DSN=wtp_data")
        self.assertEqual(len(checksumTable.tables_for_migration(con)), 1)


if __name__ == '__main__':
    unittest.main()
