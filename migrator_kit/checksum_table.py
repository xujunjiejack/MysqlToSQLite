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

    def __init__(self, ignore_tables = [], checksum_file_name = ""):
        # This file will be a json file

        if checksum_file_name == "":
            self.file_name = "checksum_table.json"
        else:
            self.file_name = checksum_file_name
        self.cur_checksum = None
        self.old_checksum = None
        self.ignore_tables = ignore_tables

    def checksum_file_exists(self):
        return os.path.exists(self.file_name) and os.stat(self.file_name).st_size > 10

    def get_all_checksum(self, sql_con):
        cur = sql_con.cursor()
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
            if table_name in self.ignore_tables:
                continue
            if table_name == "wtp_data.tables":
                continue

            # The substring truncates the prefix of those table names, which is "wtp_data."
            checksum_dict[table_name[9:]] = checksum
        return checksum_dict

    def _create_new_file_with_checksum_(self,con = None, checksum_dict = None):
        f = open(self.file_name, "w")

        if con is None and checksum_dict is None:
            raise ValueError("con and checksum_dict can't both be None")

        if checksum_dict is None:
            checksum_dict = self.get_all_checksum(con)
        json.dump(checksum_dict, f)
        f.close()

    def tables_for_migration(self, cc):
        cc.connect()

        # Save the current checksum, for later update use.
        new_table, updated_table, self.cur_checksum, self.old_checksum = self._get_tables_for_migration_(cc.sql_conn)

        cc.close_all_connection()
        return new_table, updated_table

    def create_new_checksum_for_tables(self, tables, cc):

        cc.connect()
        cur_checksum = self.get_all_checksum(sql_con=cc.sql_conn)
        final_checksum = {}
        for table in cur_checksum:
            if table in tables:
                final_checksum[table] = cur_checksum[table]

        self._create_new_file_with_checksum_(checksum_dict=final_checksum)
        cc.close_all_connection()
        return

    def update_the_checksum_of_successful_tables(self,successful_tables):
        # This should be called at the end of the program to make sure the tables that have been updated
        # in the collab will have the new checksum stored. For those tables that failed to be updated during
        # any of the process (migrator, age appender, and sanitizor), their checksum should be preserved for next try.

        # only the successful table will get assigned as the current checksum. Other table should preserve the old checksum
        final_checksum = {}
        for table in self.cur_checksum:
            if table in successful_tables:
                final_checksum[table] = self.cur_checksum[table]
            else:
                try:
                    final_checksum[table] = self.old_checksum[table]
                except KeyError as e:
                    print(e)

        self._create_new_file_with_checksum_(checksum_dict=final_checksum)
        return

    def _get_tables_for_migration_(self, con):
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
        f.close()

        updated_tables = []
        new_tables = []
        #       Compare these two
        for table in cur_checksum_dict:
            cur_checksum = cur_checksum_dict[table]
            try:
                old_checksum = old_checksum_dict[table]
                if table in self.ignore_tables:
                    continue

                if not old_checksum == cur_checksum:
                    updated_tables.append(table)

            except KeyError as e:
                if table in self.ignore_tables:
                    continue
                new_tables.append(table)
        #       and return the table that fits the criteria for migration

        return new_tables, updated_tables, cur_checksum_dict, old_checksum_dict



def test():
    checksumTable = ChecksumTable()
    con = pyodbc.connect("DSN=wtp_data")
    checksumTable.get_all_checksum(con)
