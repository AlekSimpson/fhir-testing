import pandas as pd
import warnings
from tqdm import tqdm
import traceback
import json
from util import *
warnings.filterwarnings('ignore')  # Suppresses all warnings

def find_normalize_structure(record: dict):
    record_paths = []
    meta = []
    nested_relations = []

    action_map = {
        SURFACE: meta.append,
        SHALLOW: record_paths.append,
        DEEP: nested_relations.append,
    }

    for key, _ in record.items():
        if key == 'identifier':
            nested_relations.append(key)
            continue

        depth = is_nested_relation(record, key)
        action_map[depth](key)

    meta = unfold_shallow_nested_attributes(record, meta)

    return record_paths, meta, nested_relations

def flatten_fhir_data(data: list, merge_key: str, verbose=False):
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

        if verbose:
            print(total_frame)

        return total_frame
    except Exception as e:
        raise e

def test_flatten(filename):
    try:
        synth = read_ndjson(filename + ".ndjson")
        df = flatten_fhir_data(synth, 'id', verbose=False)
        save_csv_output(df, filename + ".csv")
        print(f'--------> {filename} flatten successful.\n')
    except Exception as e:
        print(traceback.format_exc())
        print(f'--------> {filename} failed.\n')


if __name__ == "__main__":
    testfiles = ["Patient", "Claim", "ClaimResponse", "Coverage", "ExplanationOfBenefit"]

    for file in testfiles:
        test_flatten(file)



