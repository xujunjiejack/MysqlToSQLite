import unittest
from connection_coordinator import get_coordinator, DataSource
from db_exporter_kit.age_appender import AgeAppender

class MyTestCase(unittest.TestCase):


    def setUp(self):
        cc = get_coordinator(data_souce=DataSource.WTP_DATA)
        cc.sqlite_filepath = 'C:/Users/jxu259/Desktop/sqlite/wtp_collab_test.db'
        cc.connect()
        self.cc = cc
        self.ageAppender = AgeAppender(self.cc.sqlite_conn)

    def test_age_appender_all(self):

        self.ageAppender.append_age()
        self.cc.close_all_connection()

    def test_one_table(self):
        self.ageAppender.append_age("calc_s_groups")
        self.cc.close_all_connection()

if __name__ == '__main__':
    unittest.main()
