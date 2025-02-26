from src.sync_ingest.utils_ingest.utils import run_query
removal_list_merge = ['sync_constant.centres','sync_constant.channel','sync_constant.department','sync_constant.entities','sync_constant.indicator','sync_constant.itemtype','sync_constant.nationality','sync_constant.patienttype','sync_constant.servicetype','sync_merged.billingmeta','sync_merged.billingsummary','sync_merged.biomarkermeta','sync_merged.emailblacklist','sync_merged.patientidblacklist','sync_merged.items','sync_merged.mobileblacklist','sync_merged.numeric','sync_merged.patientmerged','sync_merged.patientvisit','sync_merged.patientvisitsummary','sync_merged.patientwithsrc','sync_merged.text','sync_merged.uidblacklist','sync_merged.merged_duplicate_remoteids','sync_pre_merge.email_blacklist_input','sync_pre_merge.emailindex','sync_pre_merge.mobile_blacklist_input','sync_pre_merge.mobileindex','sync_pre_merge.patientid_blacklist_input','sync_pre_merge.patientidindex','sync_pre_merge.patients_hash_index','sync_pre_merge.patients_new','sync_pre_merge.patients_new_with_ids','sync_pre_merge.uid_blacklist_input','sync_pre_merge.uidindex', 'sync_metadata.dml_stats', 'sync_metadata.audit']

removal_list_dump_raw = ['sync_dump.patients_dump', 'sync_dump.billings_dump', 'sync_dump.labreports_dump', 'sync_dump.patients_backup', 'sync_dump.billings_backup', 'sync_dump.labreports_backup','sync_rawinput.patients_json', 'sync_rawinput.billings_json', 'sync_rawinput.labreports_json', 'sync_rawinput.patients_json_backup', 'sync_rawinput.billings_json_backup', 'sync_rawinput.labreports_json_backup']

removal_list_billing = ['sync_dump.billings_backup', 'sync_rawinput.billings_json', 'sync_rawinput.billings_json_backup', 'sync_merged.patientvisit', 'sync_merged.billingmeta', 'sync_merged.items','sync_merged.billingsummary']
removal_list_labreport = ['sync_dump.labreports_backup', 'sync_rawinput.labreports_json', 'sync_rawinput.labreports_json_backup', 'sync_merged.biomarkermeta', 'sync_merged.numeric', 'sync_merged.text']

dump_watermark_defaultvalues = {'patient': '2000-01-01', 'billing': '2022-01-01', 'labreport': '2022-01-01'}


def remove_merge_data(client):
    for mergetable in removal_list_merge:
        trunc_query = f"""truncate table {mergetable}"""
        print(trunc_query)
        run_query(client, trunc_query)
    return

def update_watermark(client, task):
    updatequery = f"""update `sync_metadata.dump_watermark` set value='{dump_watermark_defaultvalues[task]}' where taskname='{task}'"""
    print(updatequery)
    run_query(client, updatequery)
    return

def remove_dump_raw_data(client):
    for dump_rawtable in removal_list_dump_raw:
        trunc_query = f"""truncate table {dump_rawtable}"""
        print(trunc_query)
        run_query(client, trunc_query)
    
    #reset entries in dump_watermark table
    for task in dump_watermark_defaultvalues:
        updatequery = f"""update `sync_metadata.dump_watermark` set value='{dump_watermark_defaultvalues[task]}' where taskname='{task}'"""
        print(updatequery)
        run_query(client, updatequery)
    return

def remove_billing_data(client):
    for billtable in removal_list_billing:
        trunc_query = f"""truncate table {billtable}"""
        print(trunc_query)
        run_query(client, trunc_query)
    return


def remove_labreport_data(client):
    for labtable in removal_list_labreport:
        trunc_query = f"""truncate table {labtable}"""
        print(trunc_query)
        run_query(client, trunc_query)
    return

    