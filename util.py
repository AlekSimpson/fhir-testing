import pandas as pd

testdata = [
    {
        "address": [
            {
                "state":"22"
            }
        ],
        "birthDate": "1952-11-17",
        "extension": [
            {
                "url":"long-url",
                "valueCoding": {
                    "code":"1",
                    "display":"White",
                    "system":"long-url"
                }
            },
            {
                "url":"long-url",
                "valueDate":"2021"
            }
        ],
        "gender":"male",
        "id":"-10000000000066",
        "identifier": [
            {
                "system":"long-url",
                "value":"-10000000000066"
            },
            {
                "system":"long-url",
                "value":"long-hash"
            },
            {
                "extension": [
                    {
                        "url": "long-url",
                        "valueCoding": {
                            "code": "current",
                            "display": "Current",
                            "system": "long-url"
                        },
                        "extra-next": [
                            {
                                'daily_scoops_sold': 245,
                                'storage_temp_celsius': -18.5,
                                'melt_time_minutes': 12,
                                'fat_percentage': 14.2,
                                'overrun_percentage': 24,
                            }
                        ]
                    }
                ],
                "system":"long-url",
                "value":"hash"
            }
        ],
        "meta": {
            "lastUpdated": "timestamp"
        },
        "name": [
            {
                "family":"Schneider199",
                "given":["Werner409"],
                "use":"usual"
            }
        ],
        "resourceType":"Patient",
        "type": {
            "coding": [
                {
                    "code": "40",
                    "display": "hospital outpatient claim",
                    "system": "long-link",
                },
            ],
        }
    },
    {
        "address": [
            {
                "state":"18"
            }
        ],
        "birthDate": "1952-11-17",
        "extension": [
            {
                "url":"long-url",
                "valueCoding": {
                    "code":"1",
                    "display":"White",
                    "system":"long-url"
                }
            },
            {
                "url":"long-url",
                "valueDate":"2021"
            }
        ],
        "gender":"female",
        "id":"-10000000000034",
        "identifier": [
            {
                "system":"long-url",
                "value":"-10000000000034"
            },
            {
                "system":"long-url",
                "value":"long-hash"
            },
            {
                "extension": [
                    {
                        "url": "long-url",
                        "valueCoding": {
                            "code": "current",
                            "display": "Current",
                            "system": "long-url"
                        },
                        "extra-next": [
                            {
                                'daily_scoops_sold': 245,
                                'storage_temp_celsius': -18.5,
                                'melt_time_minutes': 12,
                                'fat_percentage': 14.2,
                                'overrun_percentage': 24,
                            }
                        ]
                    }
                ],
                "system":"long-url",
                "value":"hash"
            }
        ],
        "meta": {
            "lastUpdated": "timestamp"
        },
        "name": [
            {
                "family":"Marks",
                "given":["Melody"],
                "use":"usual"
            }
        ],
        "resourceType":"Patient",
        "type": {
            "coding": [
                {
                    "code": "40",
                    "display": "hospital outpatient claim",
                    "system": "long-link",
                },
            ]
        }
    },
]

{
    'beneficiary': {
        'reference': 'Patient/-10000000000066'
    },
    'contract': [
        {
            'id': 'ptc-contract1'
        },
        {
            'reference': 'Coverage/part-a-contract1 reference'
        }
    ],
    'extension': [
        {
            'url': 'https://bluebutton.cms.gov/resources/variables/ms_cd', 'valueCoding': {'code': '20', 'display': 'Disabled without ESRD', 'system': 'https://bluebutton.cms.gov/resources/variables/ms_cd'}
        },
        {
            'url': 'https://bluebutton.cms.gov/resources/variables/a_trm_cd',
            'valueCoding': {
                'code': '0',
                'display': 'Not Terminated',
                'system': 'https://bluebutton.cms.gov/resources/variables/a_trm_cd'
            }
        },
        {
            'url': 'https://bluebutton.cms.gov/resources/variables/rfrnc_yr',
            'valueDate': '2021'
        }
    ],
    'grouping': {
        'subGroup': 'Medicare',
        'subPlan': 'Part A'
    },
    'id': 'part-a--10000000000066',
    'meta': {
        'lastUpdated': '2021-08-17T13:43:00.037-04:00'
    },
    'resourceType': 'Coverage',
    'status': 'active',
    'type': {
        'coding': [
            {
                'code': 'Part A',
                'system': 'Medicare'
            }
        ]
    }
}

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
    # -1  - the attribute is a single object that can be easily represented
    # 0   - the attribute is shallow and can be easily represented
    # >=1 - the attribute is deep and cannot be easily represented
    element = data[attribute]

    if isinstance(element, str) or isinstance(element, int) or isinstance(element, float):
        return SURFACE

    if isinstance(element, list):
        contain_list = does_list_have_list(element)
        contain_dict = does_list_have_dict(element)
        if not contain_list and not contain_dict:
            return SURFACE

        if contain_list:
            return DEEP

        if contain_dict:
            if len(element) == 1:
                if all(is_nested_relation(element[0], key) == SURFACE for key in element[0]):
                    return SHALLOW
            # if all(all(is_nested_relation(el, key) == SURFACE for key in el.keys()) for el in element):
            #     if len(element) == 1:
            #         return SHALLOW
            return DEEP

    if isinstance(element, dict):
        if all(is_nested_relation(element, key) == SURFACE for key in element.keys()):
            return SURFACE

        # if any(is_nested_relation(element, key) >= SHALLOW for key in element.keys()):
        #     return DEEP
        return DEEP

    print(f"!!GETTING HERE w {attribute}!!")
    return SHALLOW

def is_nested_relation_(data: dict, attribute: str):
    element = data[attribute]

    if not isinstance(element, dict) and not isinstance(element, list):
        return SURFACE

    if isinstance(element, list):
        if all(isinstance(el, dict) for el in element):
            if all(all(is_nested_relation_(obj, key) == SURFACE for key in obj.keys()) for obj in element):
                return SHALLOW

        return DEEP
    
    if isinstance(element, dict):
        if all(is_nested_relation_(element, key) == SURFACE for key in element.keys()):
            return SURFACE
        
        if all(is_nested_relation_(element, key) == SHALLOW for key in element.keys()):
            # return SHALLOW # ?
            return DEEP
        
    print(f"!!GETTING HERE w {attribute}!!")
    return SHALLOW

def unfold_shallow_nested_attributes(data: dict, meta: list):
    new_list = []
    for entry in meta:
        if not isinstance(data[entry], dict):
            new_list.append(entry)
            continue

        for key, value in data[entry].items():
            new_list.append([entry, key])
    return new_list














