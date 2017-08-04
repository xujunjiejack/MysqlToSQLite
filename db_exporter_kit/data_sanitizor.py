import logging
from db_exporter_kit.waisman_utils.db_utils import get_all_tablenames

class DataSanitizer:
    """
        Wrapper class of the sanitize function in db_exporter class.
        It performs operations such as dropping table and dropping column
        to erase the sensitive data. The categories of the sensitive data
        are listed in HIPPA training.
        For examples: 1: Names
                      2: Address, zip code
                      3: Phone numbers, fax numbers, email addresses

    """

    # This class mainly uses the function written in db_exporter.
    # This class itself maintains database connection, and an singleton of db_exporter
    # Only api now is sanitizer_collab

    def __init__(self, cc, db_exporter):
        self.cc = cc
        self.con = cc.sqlite_conn
        self.db_exporter = db_exporter

        # prepare logger
        self.logger = logging.getLogger("sql2sqlite.dataSanitizer")
        self.logger.setLevel(logging.INFO)
        fileHandler = logging.FileHandler("logs/dataSanitizer.logs")
        fileHandler.setLevel(logging.CRITICAL)
        fileHandler.setFormatter(logging.Formatter(fmt='%(asctime)s %(message)s'))
        self.logger.addHandler(fileHandler)

    # API function
    def sanitizer_collab(self, tables = None):
        self.cc.connect()
        self.con = self.cc.sqlite_conn
        successful_tables = []

        if tables is None:
            tables = get_all_tablenames(self.con)
        elif isinstance(tables, str):
            tables = [tables]
        elif not isinstance(tables, list):
            raise ValueError("tables should only be a string or list")

        for table in get_all_tablenames(self.con):
            # sanitize it
            failed_columns = []  # sanitize returns a list of failed columns or throws an exception
            try:
                failed_columns = self.db_exporter.sanitize(table, con=self.con)
            except AssertionError as e:  # happens if table could not be dropped some reason
                self.logger.critical(str(e))
            except TypeError as e:  # happens if settings file is wrong
                self.logger.critical(str(e))

            if failed_columns is None:
                pass
            elif len(failed_columns) > 0:
                self.logger.critical('__%s__:' % table)
                self.logger.critical('{')
                for col in failed_columns:
                    col_name = col[0]  # it's a tuple the first index is the column name
                    expl = str(col[1])  # the second ndex is the error
                    self.logger.critical('\t%s: %s' % (col_name, expl))
        self.cc.close_all_connection()
        return

