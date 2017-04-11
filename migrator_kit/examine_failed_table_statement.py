from connection_coordinator import get_coordinator
from migrator_kit.migrate_data import get_insert_sql_from_sql_table

coordinator = get_coordinator()
coordinator.connect()
sql_cur = coordinator.sql_cur
sqlite_cur = coordinator.sqlite_cur

table_names = ["`calc_mr_pb_t`","`data_4_zy_e2`","`data_3_le_m`","`data_rdmr_psap_t`","`user_5_dps_p_2010_0210`","`data_4_cortid`","`data_4_au_m`","`trash_nls_imagingrecruit_20100215`","`arch_int1_ccare`","`user_geo_demo_2009`","`arch_int1_s_ccare`","`gen_twins`","`data_c3_tr`","`data_3_st1`","`data_3_de_m`","`data_3_st1r`","`data_5_de_m`","`data_mr_tr`","`calc_5_hb_pb_m`","`data_3_ns_r1`","`data_3_bc_f_r1`","`data_3_zy`","`data_3_tb`","`gen_no_recruit_sort`","`data_s_tr`","`data_3_yt`","`data_mr_fe_f`","`tracsh_trash42`","`data_5_tr`","`data_rd_hb_hu_m`","`data_3_ia_e2`","`data_3_wg_1`","`data_3_oc_f`","`data_5_mr`","`user_geo_participation_with_wtp_ids`","`user_milwaukee_site_list_final`","`data_1_de_m`","`data_4_hb_hu_m`","`data_3_bp_sy_t`","`data_rd_sd_t`","`data_nc`","`data_rdmr_hdt_t`","`data_1_tr`","`data_3_bc_m_r1`","`data_s_span_de_m`","`data_palmprint_tr`","`data_3_is`","`data_3_pd_beh`","`data_rd_cg_zy`","`data_3_em`","`arch_siblings`","`data_4_re_t`","`gen_staff`","`data_4_in_f`","`arch_1_de_m`","`data_mr_bd_f`","`data_6_tr`","`data_rdmr_agn_t`","`data_3_sd`","`data_oc_t`","`user_geo_98_births_12_2004`","`data_4_ia_e2`","`data_oc_t_tap`","`data_s_span_zy`","`trash_twisttesst`","`data_3_oc_sib_m`","`data_3_fp_new`","`user_4_cort_dhea_t`","`data_c3_cg_zy`","`data_5_au_m`","`user_5_dps_t_2010_0218`","`user_5_dps_p_2010_0218`","`data_3_tg`","`trash_jlb_ph4call_97to99`","`data_4_in_m`","`user_jlb_cort4`","`data_3_hb_ph_sib_m`","`data_s_in_m`","`data_4_au_t`","`user_5_dps_t_12_15_2010`","`data_s_sd_m`","`data_2_zy`","`data_3_zy_e1`","`data_3_cg_tw_beh`","`data_r1_tr`","`data_5_hb_ph_m`","`gen_siblings`","`data_5_pd`","`user_sd_sd`","`arch_zyg`","`data_4_de_f`","`data_3_bc_t_r1`","`calc_jane_ages`","`calc_project_participation`","`calc_5_sd_t`","`data_c3_ma_t`","`data_rd_zy_t`","`data_3_oc_m`","`data_4_zy_e1`","`data_4_br_e2`","`data_4_bc_f_r1`","`data_3_bp_pp_t`","`user_nls_3_tr_update`","`data_3_bc_s_r1`","`user_geo_zyg`","`data_rd_de_m`","`data_4_ap_t`","`data_2_de_m`","`data_3_sm`","`data_birthrecord_tr`","`data_3_in_m`","`data_4_ia_e1`","`data_3_span_de_m`","`data_c3_zy_t`","`data_mr_ro_f`","`data_4_bc_t_r1`","`gen_jforms_users`","`calc_5_ti_t`","`data_3_ta`","`data_4_re_f`","`arch_int1_s_zyg`","`data_oc_old`","`data_3_is_new`","`user_jlb_ph4call_2_23_07`","`data_4_bo`","`data_3_tp`","`gen_family`","`data_3_ns`","`data_c3_au_m`","`data_rd_au_m`","`user_geodemo_2009`","`data_3_bb`","`data_6_ks3`","`data_5_in_m`","`data_c3_sd_t`","`data_4_de_m`","`data_3_sn_emo`","`trash_fsdf`","`data_3_de_f`","`data_mr_de_t`","`data_3_address_tractcode`","`data_4_hb_hu_sib_m`","`data_4_br_e1`","`data_3_ia_e1`","`data_4_re_m`","`gen_household`","`data_3_ho_i`","`data_w14_tr`","`data_c3_hb_ph_m`","`data_nm_tap`","`data_4_zy`","`gen_no_recruit`","`data_mr_fs_f`","`data_s_zy`","`data_4_bc_m_r1`","`data_3_sc_t`","`data_oc_m`","`arch_twins`","`data_rd_hb_ph_m`","`data_c3_au_t`","`data_4_bc_s_r1`","`data_nm`","`data_at_dem`","`data_oc_m_tap`","`trash_nls_geo_br_4_25`","`data_s_de_m`","`gen_secondary_contact`","`data_6_ks1`","`trash_nls_p3address_2009`","`data_c3_hb_hu_m`","`data_3_tr`","`trash_jlb_ph4call_missingdata_04_09`","`data_4_nr_t`","`data_3_rc_t`","`data_palmprints`","`data_3_hb_ph_m`","`data_4_tr`","`data_1_zy`","`data_5_au_t`","`data_4_pd`","`data_nc_tap`","`data_s_hb_hu_m`","`user_jlb_ph4call_list_04_2009`","`trash_twisttest`","`data_4_ho_i`","`user_nls_geo_wtp_br_4_25`","`data_zyg_tap`","`data_5_hb_hu_m`","`data_4_le_m`","`data_4_hb_ph_sib_m`","`data_tr_throw`","`data_at_ph`","`user_5_dps_t_2010_0208`","`data_c3_de_m`","`data_3_zy_e2`","`data_4_au_f`","`aaatable`","`data_4_hb_ph_m`","`data_rd_ds_tr`","`data_mr_ps_f`","`user_5_dps_p_12_15_2010`","`data_3_in_f`","`arch_zyg_follow up`","`arch_interview 4 response tracker`","`arch_interview 1 response tracker`","`arch_interview 3 response tracker`"]
f = open("problem_seeker.txt", "w")

for table_name in table_names:
    insert_sql = get_insert_sql_from_sql_table(sql_cur, table_name)
    f.write(table_name + ":\n")
    f.write(insert_sql + "\n")

f.close()

