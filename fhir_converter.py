import pandas as pd

def detect_record_schemes(data, prefix = ''):
    """
    Recursively detect all arrays in JSON structure
    Returns all possible record paths
    """
    array_paths = []
    sample = data[0] if data else {}
    
    def explore_dict(d, current_path):
        for key, value in d.items():
            path = current_path + [key]
            
            if isinstance(value, list) and value and isinstance(value[0], dict):
                # Found an array of objects
                array_paths.append(path)
                explore_dict(value[0], path)
            elif isinstance(value, dict):
                # Found a nested object
                explore_dict(value, path)
    
    explore_dict(sample, [])

    return array_paths

def detect_meta_scheme(records, dont_include: str):
    if len(records) == 0:
        return

    data = records[0]
    meta_scheme = []
    for key, value in data.items():
        if isinstance(value, dict):
            for k, v in value.items():
                if not isinstance(v, list):
                    meta_scheme.append([key, k])
            continue
        meta_scheme.append(key)
    meta_scheme.remove(dont_include)

    return meta_scheme

def fhir_flatten_record_nd(fhir_filepath: str, outpath: str, file_data: dict, record_scheme: list, verbose=False):
    # meta scheme dictates the columns and what data they will hold once flattened
    meta_scheme = detect_meta_scheme(file_data, dont_include=record_scheme[0])

    # print("---------"*10)
    # print(meta_scheme)
    # print(record_scheme)

    normalized_df = pd.json_normalize(file_data, record_path=record_scheme, meta=meta_scheme, record_prefix='_')

    if verbose:
        print(normalized_df.head(10))

    return normalized_df

# if __name__ == "__main__":
#     directory_prefix = "/home/alek/Desktop/projects/fhir-testing"
#     target_file = directory_prefix + "/Patient.ndjson"
#     target_output = directory_prefix + "/Patient.csv"
# 
#     input_df = pd.read_json(target_file, lines=True)
#     file_data = input_df.to_dict('records')
# 
#     # each record scheme defines how to flatten out the nested list data inside the file data
#     record_schemes = detect_record_schemes(file_data)
# 
#     # because json_normalize can only handle on record scheme at a time, we will have to generate multiple flattened dataframes for each record
#     # frames = [fhir_flatten_record_nd(target_file, target_output, file_data, scheme) for scheme in record_schemes]
# 
#     frames = []
#     for scheme in record_schemes:
#         frames.append(fhir_flatten_record_nd(target_file, target_output, file_data, scheme))
#     
#     # then we will combine all the dataframes into one master dataframe to obtain the final result
#     combined_df = pd.concat(frames, ignore_index=True)
# 
#     combined_df
# 
#     combined_df.to_csv(target_output)
# 
#     print(len(input_df['id']))
#     print(input_df['id'].nunique())
#     print(combined_df['id'].nunique())


if __name__ == "__main__":
    data = [
        {
            "state": "Florida",
            "shortname": "FL",
            "info": {"governor": "Rick Scott"},
            "county": [
                {"name": "Dade", "population": 12345},
                {"name": "Broward", "population": 40000},
                {"name": "Palm Beach", "population": 60000},
            ],
        },
        {
            "state": "Ohio",
            "shortname": "OH",
            "info": {"governor": "John Kasich"},
            "county": [
                {"name": "Summit", "population": 1234},
                {"name": "Cuyahoga", "population": 1337},
            ],
        },
    ]

    res = pd.json_normalize(data, "county", ["state", "shortname", ["info", "governor"]])
    print(res.head(20))


test = [
{
    "address": [
        {
            "state":"22"
        }
    ],
    "birthDate": "1952-11-17",
    "extension": [
        {
            "url":"https://bluebutton.cms.gov/resources/variables/race",
            "valueCoding": {
                "code":"1",
                "display":"White",
                "system":"https://bluebutton.cms.gov/resources/variables/race"
            }
        },
        {
            "url":"https://bluebutton.cms.gov/resources/variables/rfrnc_yr",
            "valueDate":"2021"
        }
    ],
    "gender":"male",
    "id":"-10000000000066",
    "identifier": [
        {
            "system":"https://bluebutton.cms.gov/resources/variables/bene_id",
            "value":"-10000000000066"
        },
        {
            "system":"https://bluebutton.cms.gov/resources/identifier/mbi-hash",
            "value":"0e239e4895a76a2aff678507b1626a7cd08d23db07280e7efa228c8b0c156d23"
        },
        {
            "extension": [
                {
                    "url": "https://bluebutton.cms.gov/resources/codesystem/identifier-currency",
                    "valueCoding": {
                        "code": "current",
                        "display": "Current",
                        "system": "https://bluebutton.cms.gov/resources/codesystem/identifier-currency"
                    }
                }
            ],
            "system":"http://hl7.org/fhir/sid/us-mbi",
            "value":"1S00E00AA66"
        }
    ],
    "meta": {
        "lastUpdated": "2021-08-17T13:43:00.037-04:00"
    },
    "name": [
        {
            "family":"Schneider199",
            "given":["Werner409"],
            "use":"usual"
        }
    ],
    "resourceType":"Patient"
}
]

