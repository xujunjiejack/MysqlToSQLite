import unittest
from migrator_kit.checksumTable import ChecksumTable
import pyodbc


class MyTestCase(unittest.TestCase):
    def test_checksum(self):
        checksumTable = ChecksumTable()
        con = pyodbc.connect("DSN=wtp_data")
        checksumTable.get_all_checksum(con)

if __name__ == '__main__':
    unittest.main()
