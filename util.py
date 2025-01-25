import pandas as pd

SURFACE = -1
SHALLOW = 0
DEEP = 1

def save_csv_output(df, filename):
    if df is None:
        return

    df.to_csv(f"output/{filename}")

def read_ndjson(filename):
    return pd.read_json(f"healthdata/{filename}", lines=True).to_dict('records')

def does_list_have_list(array: list):
    return any(isinstance(x, list) for x in array)

def does_list_have_dict(array: list):
    return any(isinstance(x, dict) for x in array)

def is_nested_relation(data: dict, attribute: str):
    element = data[attribute]

    if not isinstance(element, dict) and not isinstance(element, list):
        return SURFACE

    if isinstance(element, list):
        if len(element) <= 3 and all(isinstance(el, dict) for el in element):
            if all(all(is_nested_relation(obj, key) == SURFACE for key in obj.keys()) for obj in element):
                return SHALLOW

        return DEEP
    
    if isinstance(element, dict):
        if all(is_nested_relation(element, key) == SURFACE for key in element.keys()):
            return SURFACE
        
        if all(is_nested_relation(element, key) == SHALLOW for key in element.keys()):
            return DEEP
        
    return DEEP

def unfold_shallow_nested_attributes(data: dict, meta: list):
    new_list = []
    for entry in meta:
        if not isinstance(data[entry], dict):
            new_list.append(entry)
            continue

        for key, value in data[entry].items():
            new_list.append([entry, key])
    return new_list


testdata = [
    {
        "url":"https://bluebutton.cms.gov/resources/variables/ime_op_clm_val_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/dsh_op_clm_val_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/clm_pass_thru_per_diem_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":10}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/nch_profnl_cmpnt_chrg_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":4}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/clm_tot_pps_cptl_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/ime_op_clm_val_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/nch_bene_ip_ddctbl_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/nch_bene_pta_coinsrnc_lblty_amt",
        "valueMoney":{
            "code":"USD",
            "system":"urn:iso:std:iso:4217",
            "value":4468.03
        }
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/nch_ip_ncvrd_chrg_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":160}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/nch_ip_tot_ddctn_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":160}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/clm_pps_cptl_dsprprtnt_shr_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/clm_pps_cptl_excptn_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/clm_pps_cptl_fsp_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/clm_pps_cptl_ime_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/clm_pps_cptl_outlier_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/clm_pps_old_cptl_hld_hrmls_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/nch_drg_outlier_aprvd_pmt_amt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/nch_bene_blood_ddctbl_lblty_am","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/clm_mdcr_non_pmt_rsn_cd","valueCoding":{"system":"https://bluebutton.cms.gov/resources/variables/clm_mdcr_non_pmt_rsn_cd"}
    },
    {
        "url":"https://bluebutton.cms.gov/resources/variables/prpayamt","valueMoney":{"code":"USD","system":"urn:iso:std:iso:4217","value":0}
    }
]


# {
#     'benefitBalance': [
#         {
#             'category': {'coding': [{'code': 'medical', 'display': 'Medical Health Coverage', 'system': 'http://hl7.org/fhir/benefit-category'}]},
#             'financial': [
#                 {'type': {'coding': [{'code': 'https://bluebutton.cms.gov/resources/variables/bene_tot_coinsrnc_days_cnt', 'display': 'Beneficiary Total Coinsurance Days Count', 'system': 'https://bluebutton.cms.gov/resources/codesystem/benefit-balance'}]}, 'usedUnsignedInt': 0},
#                 {'type': {'coding': [{'code': 'https://bluebutton.cms.gov/resources/variables/clm_non_utlztn_days_cnt', 'display': 'Claim Medicare Non Utilization Days Count', 'system': 'https://bluebutton.cms.gov/resources/codesystem/benefit-balance'}]}, 'usedUnsignedInt': 0},
#                 {'type': {'coding': [{'code': 'https://bluebutton.cms.gov/resources/variables/clm_utlztn_day_cnt', 'display': 'Claim Medicare Utilization Day Count', 'system': 'https://bluebutton.cms.gov/resources/codesystem/benefit-balance'}]}, 'usedUnsignedInt': 9}
#             ]
#         }
#     ], 
#     'billablePeriod': {
#         'end': '2020-03-22',
#         'extension': [
#             {
#                 'url': 'https://bluebutton.cms.gov/resources/variables/claim_query_cd',
#                 'valueCoding': {
#                     'code': '3',
#                     'display': 'Final bill',
#                     'system': 'https://bluebutton.cms.gov/resources/variables/claim_query_cd'
#                 }
#             }
#         ],
#         'start': '2020-03-13'
#     },
# } 




