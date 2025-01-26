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

def flatten_fhir_data(data: list, merge_key: str):
    try:
        record_paths, meta, nested_relations = find_normalize_structure(data[0])

        if len(record_paths) == 0:
            record_paths = [None]
            data = pd.DataFrame(data).drop(columns=nested_relations).to_dict('records')

        frames = [pd.json_normalize(data, record_paths[0], meta, record_prefix=f"{record_paths[0]}.")]
        total_frame = frames[0]
        for index in tqdm(range(1, len(record_paths))):
            path = record_paths[index]
            frames.append(pd.json_normalize(data, path, meta, record_prefix=f"{path}."))

            cols_to_use = frames[index].columns.difference(total_frame.columns)
            cols_to_use = cols_to_use.append(pd.Index([merge_key]))
            total_frame = pd.merge(total_frame, frames[index][cols_to_use], on=merge_key)

        return total_frame
    except Exception as e:
        raise e

def flatten():
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
        'flatten': flatten,
        'test': test_flatten,
    }

    if args[1] not in command_map:
        return

    command_map[args[1]]()


if __name__ == "__main__":
    parse_program_args()
