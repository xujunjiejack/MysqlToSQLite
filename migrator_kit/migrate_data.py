import logging
import sqlite3
from datetime import datetime
from connection_coordinator import get_coordinator, DataSource
from util.fetch_all_table_name import read_table_names_from_sql_cursor


logging.basicConfig(filename="failed_insert_record", level=logging.INFO)


def get_field_names(desc): return [x[0] for x in desc]

def clean_record(record):
    """
        Given a tuple of record that stores data, this method tries to convert each value in the
        record to string that can be used in sql statement.
        e.g: none value element will be presented as NULL in sql
             string value element will be surrounded with double quote (")
    :param record: a tuple of value fetched from database
    :return: a cleaned version of record value that can be recognized by sql
    """
    # record is a tuple
    record_list =[]
    for element in record:
        if element is None:
            record_list.append("NULL")
            continue

        if type(element) is datetime:
            record_list.append('"%s"' % element)
            continue

        if type(element) is str:
            if "\"" in element:
                element = element.replace("\"", "")
            record_list.append('"%s"' % element)
            continue

        record_list.append("%s" % element)

    return record_list


def join_each_record(record):
    """
        convert everything inside record to string,add original string with '', and change None to NULL
    :param record:
    :return:
    """
    record = clean_record(record)
    delimiter = ","
    return "(%s)" % delimiter.join(record)


def join_records_to_query(records):

    records_list = [join_each_record(x) for x in records]
    delimiter = ","
    return delimiter.join(records_list)


def join_field_names_with_comma_and_quotes(field_names):
    """
        Each field will be surrounded with ` to indicate that this is a column
    :param field_names:
    :return:
    """
    #field_names = ["`%s`"%x for x in field_names]
    return ",".join(map(lambda x: "`%s`"%x, field_names))


def create_insert_statement(table_name, desc, records):
    field_names = get_field_names(desc)
    fieldnames_query = join_field_names_with_comma_and_quotes(field_names)
    records_query = join_records_to_query(records)
    stmt = "INSERT INTO {0} ({1}) VALUES {2}".format(table_name, fieldnames_query, records_query)
    return stmt


def get_insert_sql_from_sql_table(sql_cur, table_name):
    get_all_data_query = "SELECT * FROM %s" % table_name
    sql_cur.execute(get_all_data_query)
    desc = sql_cur.description
    records = sql_cur.fetchall()

    return create_insert_statement(table_name, desc, records)
# The idea is to fetch data from the mysql database
    # test table: calc_1_bi_f
    # assume the table has been created
    # USE SELECT * FROM calc_1_bi_f to get all information

class Migrater:

    # support migrating data from wtp_data, but be careful, it's not good to migrate from wtp_data due to the
    # sensitive data it has
    def __init__(self, data_source=DataSource.WTP_COLLAB, exporter = None):
        # prepare logger
        self.logger = logging.getLogger("sql2sqlite.migration")
        self.logger.setLevel(logging.INFO)
        fileHandler = logging.FileHandler("migration.logs")
        fileHandler.setLevel(logging.CRITICAL)
        fileHandler.setFormatter(logging.Formatter(fmt='%(asctime)s %(message)s'))
        self.logger.addHandler(fileHandler)
        self.logger.critical("------ Start migrating ------")

        self.coordinator = get_coordinator(data_source)
        self.coordinator.connect()
        self.sql_cur = self.coordinator.sql_cur
        self.sqlite_cur = self.coordinator.sqlite_cur

        self.already_added_table_list = []
        self.weird_error_table_dic = {}
        self.critical_failed_table = set()
        self.exporter = exporter

    def get_insert_statment(self, table_name, fieldnames, records):
        return "INSERT INTO {0} ({1}) VALUES {2}".format(table_name, fieldnames, records)

    def migrate_some_tables(self, table_names):
        """
            Accept a list of table names, and add those from wtp_collab to the sqlite datafile

            TODO: truncate the old data to import the new one? The time spent in inserting the table is really trivial

        :param table_names: list of table names
        :return:
        """

        #table_names = ["`calc_mr_pb_t`","`data_4_zy_e2`","`data_3_le_m`","`user_5_dps_p_2010_0210`","`data_4_cortid`","`data_4_au_m`","`trash_nls_imagingrecruit_20100215`","`arch_int1_ccare`","`user_geo_demo_2009`","`arch_int1_s_ccare`","`gen_twins`","`data_c3_tr`","`data_3_st1`","`data_3_de_m`","`data_3_st1r`","`data_5_de_m`","`data_mr_tr`","`calc_5_hb_pb_m`","`data_3_ns_r1`","`data_3_bc_f_r1`","`data_3_zy`","`data_3_tb`","`gen_no_recruit_sort`","`data_s_tr`","`data_3_yt`","`data_mr_fe_f`","`tracsh_trash42`","`data_5_tr`","`data_rd_hb_hu_m`","`data_3_ia_e2`","`data_3_wg_1`","`data_3_oc_f`","`data_5_mr`","`user_geo_participation_with_wtp_ids`","`user_milwaukee_site_list_final`","`data_1_de_m`","`data_4_hb_hu_m`","`data_3_bp_sy_t`","`data_rd_sd_t`","`data_nc`","`data_rdmr_hdt_t`","`data_1_tr`","`data_3_bc_m_r1`","`data_s_span_de_m`","`data_palmprint_tr`","`data_3_is`","`data_3_pd_beh`","`data_rd_cg_zy`","`data_3_em`","`arch_siblings`","`data_4_re_t`","`gen_staff`","`data_4_in_f`","`arch_1_de_m`","`data_mr_bd_f`","`data_6_tr`","`data_rdmr_agn_t`","`data_3_sd`","`data_oc_t`","`user_geo_98_births_12_2004`","`data_4_ia_e2`","`data_oc_t_tap`","`data_s_span_zy`","`trash_twisttesst`","`data_3_oc_sib_m`","`data_3_fp_new`","`user_4_cort_dhea_t`","`data_c3_cg_zy`","`data_5_au_m`","`user_5_dps_t_2010_0218`","`user_5_dps_p_2010_0218`","`data_3_tg`","`trash_jlb_ph4call_97to99`","`data_4_in_m`","`user_jlb_cort4`","`data_3_hb_ph_sib_m`","`data_s_in_m`","`data_4_au_t`","`user_5_dps_t_12_15_2010`","`data_s_sd_m`","`data_2_zy`","`data_3_zy_e1`","`data_3_cg_tw_beh`","`data_r1_tr`","`data_5_hb_ph_m`","`gen_siblings`","`data_5_pd`","`user_sd_sd`","`arch_zyg`","`data_4_de_f`","`data_3_bc_t_r1`","`calc_jane_ages`","`calc_project_participation`","`calc_5_sd_t`","`data_c3_ma_t`","`data_rd_zy_t`","`data_3_oc_m`","`data_4_zy_e1`","`data_4_br_e2`","`data_4_bc_f_r1`","`data_3_bp_pp_t`","`user_nls_3_tr_update`","`data_3_bc_s_r1`","`user_geo_zyg`","`data_rd_de_m`","`data_4_ap_t`","`data_2_de_m`","`data_3_sm`","`data_birthrecord_tr`","`data_3_in_m`","`data_4_ia_e1`","`data_3_span_de_m`","`data_c3_zy_t`","`data_mr_ro_f`","`data_4_bc_t_r1`","`gen_jforms_users`","`calc_5_ti_t`","`data_3_ta`","`data_4_re_f`","`arch_int1_s_zyg`","`data_oc_old`","`data_3_is_new`","`user_jlb_ph4call_2_23_07`","`data_4_bo`","`data_3_tp`","`gen_family`","`data_3_ns`","`data_c3_au_m`","`data_rd_au_m`","`user_geodemo_2009`","`data_3_bb`","`data_6_ks3`","`data_5_in_m`","`data_c3_sd_t`","`data_4_de_m`","`data_3_sn_emo`","`trash_fsdf`","`data_3_de_f`","`data_mr_de_t`","`data_3_address_tractcode`","`data_4_hb_hu_sib_m`","`data_4_br_e1`","`data_3_ia_e1`","`data_4_re_m`","`gen_household`","`data_3_ho_i`","`data_w14_tr`","`data_c3_hb_ph_m`","`data_nm_tap`","`data_4_zy`","`gen_no_recruit`","`data_mr_fs_f`","`data_s_zy`","`data_4_bc_m_r1`","`data_3_sc_t`","`data_oc_m`","`arch_twins`","`data_rd_hb_ph_m`","`data_c3_au_t`","`data_4_bc_s_r1`","`data_nm`","`data_at_dem`","`data_oc_m_tap`","`trash_nls_geo_br_4_25`","`data_s_de_m`","`gen_secondary_contact`","`data_6_ks1`","`trash_nls_p3address_2009`","`data_c3_hb_hu_m`","`data_3_tr`","`trash_jlb_ph4call_missingdata_04_09`","`data_4_nr_t`","`data_3_rc_t`","`data_palmprints`","`data_3_hb_ph_m`","`data_4_tr`","`data_1_zy`","`data_5_au_t`","`data_4_pd`","`data_nc_tap`","`data_s_hb_hu_m`","`user_jlb_ph4call_list_04_2009`","`trash_twisttest`","`data_4_ho_i`","`user_nls_geo_wtp_br_4_25`","`data_zyg_tap`","`data_5_hb_hu_m`","`data_4_le_m`","`data_4_hb_ph_sib_m`","`data_tr_throw`","`data_at_ph`","`user_5_dps_t_2010_0208`","`data_c3_de_m`","`data_3_zy_e2`","`data_4_au_f`","`aaatable`","`data_4_hb_ph_m`","`data_rd_ds_tr`","`data_mr_ps_f`","`user_5_dps_p_12_15_2010`","`data_3_in_f`","`arch_zyg_follow up`","`arch_interview 4 response tracker`","`arch_interview 1 response tracker`","`arch_interview 3 response tracker`"]
        too_long_to_get_loaded = ["`data_rdmr_psap_t`"]
        self.migrate_with_one_record_a_time(table_names=table_names)
        self.dump_error_tables()
        self.coordinator.close_all_connection()

    def migrate_all_tables(self):
        """
            Migrate all tables from the source database to the destination sqlite database
        :return:
        """
        table_names = read_table_names_from_sql_cursor(self.sql_cur)
        table_needed_exported = filter(lambda tablename:
                                            self.exporter.want_to_export(tablename.strip("`"))
                                        , table_names)
        self.migrate_with_one_record_a_time(table_needed_exported)
        self.dump_error_tables()
#        self.coordinator.close_all_connection()

    def migrate_with_one_record_a_time(self, table_names):

        for table_name in table_names:
            self.logger.info("-----Migrating table: {0}------".format(table_name))

            try:
                records, desc = self.fetch_all_data_of(table_name)
            except Exception as e:
                self.logger.warn("error happens when fetching data from mysql database. Msg: %s" %e)
                continue

            self.insert_records(table_name, join_field_names_with_comma_and_quotes(get_field_names(desc)), records)

            self.coordinator.commit()
            self.logger.info("#################################################")

        self.coordinator.close_all_connection()

        self.dump_tracker()

    def fetch_all_data_of(self, table_name):
        self.sql_cur.execute("SELECT * FROM %s" % table_name)
        return self.sql_cur.fetchall(), self.sql_cur.description

    def insert_records(self, table_name, fieldnames_query, records):
        # For giving feedback on how many lines have been input.

        self.logger.info("Start inserting data to table: {0}".format(table_name))

        for record_num, record in enumerate(records):
            self.logger.info("record {0}/{1}".format(record_num, len(records)))

            stmt = self.get_insert_statment(table_name, fieldnames_query, join_each_record(record))
            self.execute_insert_sql(stmt, table_name)


    def execute_insert_sql(self, insert_sql, table_name):

        # self.logger.info("Start inserting data to table: {0}".format(table_name))
        try:
            self.sqlite_cur.execute(insert_sql)
        except sqlite3.IntegrityError as e:
            if table_name not in self.already_added_table_list:
                self.already_added_table_list.append(table_name)
            self.logger.warn("Error happens when inserting for table {0}".format(table_name))
            self.logger.warn("Due to {0}".format(e))

        except Exception as e:
            if table_name not in self.weird_error_table_dic:
                self.weird_error_table_dic[table_name] = "%s" % e
            self.logger.critical("Error: {0}, record: {1}, msg: {2}".format(table_name, insert_sql, e))
            self.logger.warn("Error happens when inserting for table {0}".format(table_name))
            self.logger.warn("Due to {0}".format(e))
            self.critical_failed_table.add(table_name)

       # finally:
            #self.coordinator.commit()

        #self.logger.debug("---------------------------------------------------------\n")

    def dump_tracker(self):
        with open("table_insert_tracker.txt", mode="w") as f:
            f.write("ALREADY_ADDED_TABLES\n")
            f.write("{0}".format(self.already_added_table_list))

            f.write("TABLES NOT WORK\n")
            f.write("{0}".format(self.weird_error_table_dic))

    def dump_error_tables(self):
        self.logger.critical("Critial failed table: {tables}".format(tables=self.critical_failed_table))


    def migrate(self, table_names):
        # Outdated function
        for table_name in table_names:

            self.logger.info("Migrating table: {0}".format(table_name))

            insert_sql = get_insert_sql_from_sql_table(self.sql_cur, table_name)

            self.logger.debug("The insert statement is {0}".format(insert_sql))

            self.execute_insert_sql(insert_sql, table_name)

        self.coordinator.close_all_connection()

        self.dump_tracker()
# and then create insert sql statement with multiple input value
    # assume this format of insert sql query can work
    # "INSERT INTO calc_1_bi_f () VALUES (), (), ();"

# ask sqlite cur to execute it

