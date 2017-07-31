# This checksum table will create a local file to store the checksum. And when the migrator runs, the migrator will consult
# the checksum table to decide whether the migration for the table is necessary. The decision process is to fetch
# checksum for all of the mysql tables, and compare it with the checksum stored. If it doesn't exist or the checksum has
# been changed, the table will be returned as the part of migration process. If not, then nothing will happen.
# Then the checksum table will get updated.

# Checksum shouldn't be none if it's a table we create. So we can ignore tables with checksum "None".
# This table (wtp_data.tables 2827515988), which seems to be a system table, should also get skipped, because
# I can't migrate it.

import os
import sys
import pyodbc
import json

class ChecksumTable:

    def __init__(self):
        # This file will be a json file
        self.file_name = "checksum_table.json"

    def checksum_file_exists(self):
        return os.path.exists(self.file_name) or os.stat(self.file_name).st_size > 10

    def get_all_checksum(self, con):
        cur = con.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables;")
        rows = cur.fetchall()
        # Dictionary
        checksum_dict = {}

        for row in rows:
            table_name = row[0]
            cur.execute("CHECKSUM TABLE `{0}`;".format(table_name))
            checksum_row = cur.fetchone()
            table_name = checksum_row[0]
            checksum = checksum_row[1]
            if checksum is None:
                continue
            if table_name == "wtp_data.tables":
                continue

            # The substring truncates the prefix of those table names, which is "wtp_data."
            checksum_dict[table_name[9:]] = checksum
        return checksum_dict

    def _create_new_file_with_checksum_(self,con):
        f = open(self.file_name, "w")
        checksum_dict = self.get_all_checksum(con)
        json.dump(checksum_dict, f)
        f.close()

    def tables_for_migration(self, con):
        # If the json file doesn't exist, it will directly produce one. Then all of the tables will be
        # returned for migration
        if not self.checksum_file_exists():
            self._create_new_file_with_checksum_(con)
            return

        # If the json file exists,
        #       The new checksum will be fetched from the wtp_data
        cur_checksum_dict = self.get_all_checksum(con)

        #       The class will then read the file into the memory.
        f = open(self.file_name, "r")
        old_checksum_dict = json.load(f)
        #       Compare these two

        #       and return the table that fits the criteria for migration

        pass

def test():
    checksumTable = ChecksumTable()
    con = pyodbc.connect("DSN=wtp_data")
    checksumTable.get_all_checksum(con)
