import pandas as pd
import warnings
import traceback
import json
from tqdm import tqdm
from util import *
import sys
import os
warnings.filterwarnings('ignore')  # Suppresses all warnings

def find_normalize_structure(record: dict):
    record_paths = []
    meta = []
    nested_relations = []

    action_map = {
        META: meta.append,
        PATH: record_paths.append,
        NEST: nested_relations.append,
    }

    for key, _ in record.items():
        if key == 'identifier' or key == 'organization' or key == 'insurance' or key == 'insurer':
            nested_relations.append(key)
            continue

        depth = is_nested_relation(record, key)
        action_map[depth](key)

    meta = unfold_shallow_nested_attributes(record, meta)

    return record_paths, meta, nested_relations

# 'identifier' : [
#     {
#         'system': 'https://bluebutton.cms.gov/resources/variables/bene_id',
#         'value': '-10000000000066'
#     },
#     {
#         'system': 'https://bluebutton.cms.gov/resources/identifier/mbi-hash',
#         'value': '0e239e4895a76a2aff678507b1626a7cd08d23db07280e7efa228c8b0c156d23'
#     },
#     {
#         'extension': [
#             {
#                 'url': 'https://bluebutton.cms.gov/resources/codesystem/identifier-currency',
#                 'valueCoding': {
#                     'code': 'current',
#                     'display': 'Current',
#                     'system': 'https://bluebutton.cms.gov/resources/codesystem/identifier-currency'
#                 }
#             }
#         ],
#         'system': 'http://hl7.org/fhir/sid/us-mbi',
#         'value': '1S00E00AA66'
#     }
# ]

def generate_data_relations(data: list, relational_fields: list):
    field = relational_fields[1]

    # get base frame
    df = pd.json_normalize(data, record_path=field, meta=['id'])

    # test = df.to_dict('records') <--- use this list to scan for the record_path attributes (basically just want to check if any of them are a list)
    # print(test[0])

    # get all pathed nested attribute frame data
    df_dict = pd.DataFrame(df[['extension', 'id']]).to_dict('records')

    # normalize it
    norm_df = pd.json_normalize(df_dict, 'extension', meta=['id'])

    # merge it with the base frame
    cols_to_use = df.columns.difference(['extension'])
    result = pd.merge(df[cols_to_use], norm_df, on='id')
    result = result.add_prefix('extension.')
    print(result)

    save_csv_output(result, 'TESTOUTPUT.csv')

    # for field in relational_fields:
    #     print(f'------- {field} -------')
    #     df = pd.json_normalize(data, record_path=field, meta=['id'])
    #     print(df)
    #     print(df.to_dict('records')[2])
    #     # record_paths, meta, nested = find_normalize_structure(data[0][field])

    #     # print(f'record_paths = {record_paths}')
    #     # print(f'meta = {meta}')
    #     # print(f'nested = {nested}')

def flatten_fhir_data(data: list, merge_key: str):
    try:
        record_paths, meta, nested_relations = find_normalize_structure(data[0])

        if len(record_paths) == 0:
            record_paths = [None]

        frames = [pd.json_normalize(data, record_paths[0], meta, record_prefix=f"{record_paths[0]}.")]
        total_frame = frames[0]
        for index in tqdm(range(1, len(record_paths))):
            path = record_paths[index]
            frames.append(pd.json_normalize(data, path, meta, record_prefix=f"{path}."))

            cols_to_use = frames[index].columns.difference(total_frame.columns)
            cols_to_use = cols_to_use.append(pd.Index([merge_key]))
            total_frame = pd.merge(total_frame, frames[index][cols_to_use], on=merge_key)

        generate_data_relations(data, nested_relations)

        return total_frame
    except Exception as e:
        raise e

def flatten_files():
    files = os.listdir('./input/')
    for file in files:
        try:
            filename = file.split('.')[0]
    
            data = read_ndjson(file)
            df = flatten_fhir_data(data, 'id')
            save_csv_output(df, filename + ".csv")
            print(f'torch: {filename} flatten successful.\n')
        except Exception as e:
            print(traceback.format_exc())
            print(f'torch: {filename} failed.\n')
    print('torch: done.')
    
def test_flatten():
    files = os.listdir('./testdata/')

    for file in files:
        try:
            filename = file.split('.')[0]

            synth = read_ndjson(file)
            df = flatten_fhir_data(synth, 'id')
            save_csv_output(df, filename + ".csv")
            print(f'torch: {filename} flatten successful.\n')
        except Exception as e:
            print(traceback.format_exc())
            print(f'torch: {filename} failed.\n')
    print('torch: done.')

def help_user():
    print("Available commands:")
    print("\t./torch help: prints help menu")
    print("\t./torch flatten: reads all .ndjson files in ./input/ and flattens them to .csv")
    print("\t./torch test: runs the flatten operation on the test data inside ./testdata/")

def parse_program_args():
    args = sys.argv

    if len(args) <= 1: 
        print("No program inputs provided. Use `./torch help` for more information.")
        return

    command_map = {
        'help': help_user,
        'flatten': flatten_files,
        'test': test_flatten,
    }

    if args[1] not in command_map:
        return

    command_map[args[1]]()


if __name__ == "__main__":
    # parse_program_args()

    synth = read_ndjson('Patient.ndjson')
    df = flatten_fhir_data(synth, 'id')







