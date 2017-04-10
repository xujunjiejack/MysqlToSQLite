import unittest
from connection_coordinator import get_coordinator, DataSource
import db_exporter_kit.waisman_utils.db_utils as db_utils
from db_exporter_kit.export_db import get_db_exporter
from util.fetch_all_table_name import read_sqlite_tables_from_sqlite_cursor

class MyTestCase(unittest.TestCase):

    def setUp(self):
        cc = get_coordinator(data_souce=DataSource.WTP_DATA)
        cc.sqlite_filepath = 'C:/Users/jxu259/Desktop/sqlite/wtp_collab_test.db'
        cc.connect()
        self.cc = cc

    def test_something(self):
        self.assertEqual(True, False)

    def test_drop_column_sqlite3(self):
        test_table = "calc_3_cb_f"
        drop_columns = ["fatherage", "motherage"]
        db_utils.drop_columns(self.cc.sqlite_conn, test_table, drop_columns)
        return

    def test_sanitize(self):
        db_exporter = get_db_exporter()
        for table in read_sqlite_tables_from_sqlite_cursor(self.cc.sqlite_conn.cursor()):
            # sanitize it
            failed_columns = []  # sanitize returns a list of failed columns or throws an exception
            try:

                failed_columns = db_exporter.sanitize(table, con=self.cc.sqlite_conn)
            except AssertionError as e:  # happens if table could not be dropped some reason
                print(str(e))
            except TypeError as e:  # happens if settings file is wrong
                print(str(e))

            if failed_columns is None:
                pass
            elif len(failed_columns) > 0:
                print('__%s__:' % table)
                print('{')
                for col in failed_columns:
                    col_name = col[0]  # it's a tuple the first index is the column name
                    expl = str(col[1])  # the second ndex is the error
                    print('\t%s: %s' % (col_name, expl))

if __name__ == '__main__':
    unittest.main()
