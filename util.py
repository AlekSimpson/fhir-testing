import pandas as pd
from enum import Enum

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

[
    {
        "system":"http://hl7.org/fhir/sid/us-mbi",
        "type":{
            "coding":[
                {
                    "code":"MC",
                    "display":"Patient's Medicare Number",
                    "system":"http://terminology.hl7.org/CodeSystem/v2-0203"
                }
            ]
        },
        "value":"1S00E00AM35"
    }
]

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
            if len(element) > 1:
                return DEEP

            if all(is_nested_relation(element, key) == SURFACE for key in element.keys()):
                return SHALLOW

            return DEEP

    if isinstance(element, dict):
        if len(element.keys()) <= 1 and all(is_nested_relation(element, key) == SURFACE for key in element.keys()):
            return SURFACE

        if len(element.keys()) > 1 or any(is_nested_relation(element, key) >= SHALLOW for key in element.keys()):
            return DEEP
        return DEEP

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

















