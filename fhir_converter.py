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

    for key, _ in record.items():
        depth = is_nested_relation(record, key)
        if depth == SURFACE:
            meta.append(key)
        elif depth == SHALLOW:
            record_paths.append(key)
        elif depth == DEEP:
            nested_relations.append(key)

    meta = unfold_shallow_nested_attributes(record, meta)

    return record_paths, meta, nested_relations

def flatten_fhir_data(data: list, merge_key: str, verbose=False):
    try:
        record_paths, meta, nested_relations = find_normalize_structure(data[0])

        if len(record_paths) == 0:
            record_paths = [None]
            data = pd.DataFrame(data).drop(columns=nested_relations).to_dict('records')

        if verbose:
            print(f'paths = {record_paths}')
            print(f'meta = {meta}')
            print(f'nested = {nested_relations}')

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
        print(traceback.format_exc())
        print("flattening failed--------------")


if __name__ == "__main__":
    # synthdata = read_ndjson("ExplanationOfBenefit.ndjson")
    # synthdata = read_ndjson("Patient.ndjson")
    # synthdata = read_ndjson("Claim.ndjson")
    # synthdata = read_ndjson("ClaimResponse.ndjson")
    synthdata = read_ndjson("Coverage.ndjson")

    # x = flatten_fhir_data(cov_synthdata, 'id', verbose=True)
    x = flatten_fhir_data(synthdata, 'id', verbose=True)
    save_csv_output(x, 'synthdata.csv')

    # save_csv_output(x, 'patient.csv')








# ClaimResponse: id contained somewhere in each record
# Coverage: id contained somewhere in each record
# Claim: has id
