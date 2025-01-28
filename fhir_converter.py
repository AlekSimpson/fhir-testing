import pandas as pd
import warnings
import traceback
import json
from tqdm import tqdm
from util import *
import sys
import os
warnings.filterwarnings('ignore')  # Suppresses all warnings

MERGE_KEY = 'id'
csv_writer = CSVWriter()

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

def generate_data_relations(data: list, relation_fields: list, merge_key: str):
    # data_df = pd.DataFrame(data)
    # data_df = data_df.rename(columns={'id':'id914'})
    # data = data_df.to_dict('records')

    return generate_data_relations_helper(data, relation_fields, merge_key)

def generate_data_relations_helper(data: list, relation_fields: list, merge_key: str):
    if len(relation_fields) == 0:
        return pd.DataFrame(data)

    print(relation_fields)
    field = relation_fields[0]

    data_df = pd.DataFrame(data)
    old_cols = data_df.columns

    print(field)
    df = pd.json_normalize(data, field, meta=[merge_key])
    cols =  data_df.columns.difference([field])
    df = pd.merge(data_df[cols], df, on=merge_key)
    print(df)
    print('-'*150)

    relation_fields.remove(field)
    return generate_data_relations_helper(df.to_dict('records'), relation_fields, merge_key)

def generate_data_relations_(data: list, relational_fields: list, merge_key: str):
    # generate a csv file for each nested relation table
    for field in relational_fields:
        print(field)

        # rename id field to something completely unique for the duration of the flattening operation to ensure that there are no collisions with the record id and some nested sub id field (also titled 'id')
        data_df = pd.DataFrame(data)
        data_df = data_df.rename(columns={'id':'id914'})
        data = data_df.to_dict('records')
        merge_key = 'id914'

        # get base frame
        if isinstance(data[0][field], dict):
            df = pd.json_normalize(data, meta=[field, merge_key])
        else:
            df = pd.json_normalize(data, record_path=field, meta=[merge_key])
        print('==============')
        print(df.head())
        print('==============')

        # creates a list that we use to scan for the record_path attributes (basically just want to check if any of them are a list; if so, add them to our set)
        records = df.to_dict('records')
        path_records = set([])
        for record in records:
            for key in record.keys():
                if isinstance(record[key], list):
                    path_records.add(key)
        path_records = list(path_records)
        print(path_records)
        print('==============')
        if len(path_records) == 0:
            df = df.add_prefix(f'{field}.')
            csv_writer.save_csv_output(df, f'{field}.csv')
            print(df.head())
            continue

        # get all pathed nested attribute frame data
        records_with_id = path_records.copy()
        records_with_id.append(merge_key)
        df_dict = pd.DataFrame(df[records_with_id]).to_dict('records')

        # normalize it
        for path_record in path_records:
            norm_df = pd.json_normalize(df_dict, path_record, meta=[merge_key])

            # merge it with the base frame (e.g. `df`)
            cols_to_use = df.columns.difference(path_records) # take difference of all path records because we don't want the columns to show up anywhere in any of these combined frames
            result_df = pd.merge(df[cols_to_use], norm_df, on=merge_key)
        result_df = result_df.add_prefix(f'{field}.')

        # rename the merge_key to the original id

        csv_writer.save_csv_output(result_df, f'{field}.csv')
        print(result_df.head())

def flatten_fhir_data(data: list, merge_key: str):
    try:
        record_paths, meta, nested_relations = find_normalize_structure(data[0])

        if len(nested_relations) == 0:
            csv_writer.current_bundle_directory = None

        # if the array is empty then set its first element to None so that it is passing valid input into pandas's json_normalize function
        if len(record_paths) == 0:
            record_paths = [None]

        # create the primary flattened relation
        frames = [pd.json_normalize(data, record_paths[0], meta, record_prefix=f"{record_paths[0]}.")]
        total_frame = frames[0]
        for index in tqdm(range(1, len(record_paths))):
            path = record_paths[index]
            frames.append(pd.json_normalize(data, path, meta, record_prefix=f"{path}."))

            cols_to_use = frames[index].columns.difference(total_frame.columns)
            cols_to_use = cols_to_use.append(pd.Index([merge_key]))
            total_frame = pd.merge(total_frame, frames[index][cols_to_use], on=merge_key)

        generate_data_relations(data, nested_relations, merge_key)

        # add the foreign key connections to the nested relation table files
        for nested in nested_relations:
            total_frame[nested] = total_frame[merge_key].copy()

        return total_frame
    except Exception as e:
        raise e

def flatten_files(target_directory='input/'):
    files = os.listdir(target_directory)
    for file in files:
        try:
            filename = file.split('.')[0] # strip the file extension from the filename
    
            data = read_ndjson(file)
            csv_writer.current_bundle_directory = f'{filename}-relations'
            df = flatten_fhir_data(data, MERGE_KEY)
            csv_writer.save_csv_output(df, filename + ".csv")
            print(f'torch: {filename} flatten successful.\n')
        except Exception as e:
            print(traceback.format_exc())
            print(f'torch: {filename} failed.\n')
    print('torch: done.')
    
def test_flatten():
    flatten_files('testdata/')

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

def get_relations(df):
    """
    Returns a set of column names where at least one element is a Python list.
    """
    list_columns = []
    for col in df.columns:
        # Check if column is object dtype (lists are stored as objects)
        if df[col].dtype == 'object':
            # Check if any element in the column is a list
            if df[col].apply(lambda x: isinstance(x, list)).any():
                list_columns.append(col)
    return set(list_columns)

def get_attributes_relations(df):
    relations = get_relations(df)
    attributes = set(df.columns)
    return list(attributes.difference(relations)), list(relations)

def prototype(data: list, key: str):
    master_attributes, master_relations = get_attributes_relations(pd.DataFrame(data))
    if len(master_relations) == 0:
        print(f'ending attributes = {master_attributes}')
        return pd.DataFrame(data)

    relation = master_relations[0]
    attributes = master_attributes.copy()
    attributes.extend(master_relations[1:])

    df = pd.json_normalize(data, relation, attributes, record_prefix=f'{relation}.')
    return prototype(df.to_dict('records'), key)

def group_columns_by_prefix(df, delimiter='.'):
    """
    Groups DataFrame columns by their prefix (split by the first occurrence of `delimiter`).
    Columns are ordered by the first occurrence of their prefix in the original DataFrame.
    """
    prefix_order = []  # Tracks the order of first occurrence of prefixes
    grouped_columns = {}  # Maps prefixes to their corresponding columns
    
    for col in df.columns:
        # Split column name into prefix and the rest
        parts = col.split(delimiter, 1)
        prefix = parts[0]
        
        # Track the first occurrence order of prefixes
        if prefix not in grouped_columns:
            grouped_columns[prefix] = []
            prefix_order.append(prefix)
        
        grouped_columns[prefix].append(col)
    
    # Create the new column order by grouping prefixes
    new_columns = [
        col 
        for prefix in prefix_order 
        for col in grouped_columns[prefix]
    ]
    
    return df[new_columns]

if __name__ == "__main__":
    # parse_program_args()

    data = read_ndjson('Patient.ndjson')
    df = prototype(data, 'id')
    df = group_columns_by_prefix(df)
    column_to_move = df.pop('id')
    df.insert(0, 'id', column_to_move)
    csv_writer.save_csv_output(df, 'Patient.csv')
    print(df)





















