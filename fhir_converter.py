import pandas as pd
import sys
from util import *
from pathlib import Path

csv_writer = CSVWriter()
    
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

def get_complex_columns(data: list, filename: str):
    df = pd.DataFrame(data)

    # Check which columns contain dictionary elements
    dict_columns = df.map(lambda x: isinstance(x, (list, dict))).any()
    
    # Filter columns that contain at least one dictionary
    columns_with_dicts = dict_columns[dict_columns].index.tolist()

    complex_columns = []

    for column in columns_with_dicts:
        max_depth = max(depth_probe(item) for item in df[column].tolist())
        max_width = max(width_probe(item) for item in df[column].tolist())
        if max_width > 2 and max_depth > 4:
            complex_columns.append(column)
    
    if len(complex_columns) != 0:
        csv_writer.current_bundle_directory = f'{filename}-relations'

    return complex_columns

def flatten(data: list, filename: str):
    # check for any columns that need to be flattened as sub relations
    complex_column_names = get_complex_columns(data, filename)

    # remove the relations that are too big for the main dataframe
    data_df = pd.DataFrame(data)
    complex_columns = []
    for column in complex_column_names:
        complex_columns.append(data_df[column].copy())
        data_df = data_df.drop(column, axis=1)
    data = data_df.to_dict('records')

    # flatten the main data
    result_df = flatten_helper(data)
    result_df = group_columns_by_prefix(result_df)

    # if any sub relations are needed then flatten those out
    for col_name, column in zip(complex_column_names, complex_columns):
        column_with_ids = pd.concat([column.to_frame(), data_df['id']], axis=1)
        nested_df = flatten_helper(column_with_ids.to_dict('records'))
        csv_writer.save_csv_output(nested_df, f'{col_name}.csv')

        # add the sub relation keys as columns in the main dataframe
        result_df[col_name] = result_df['id'].copy()
    
    column_to_move = result_df.pop('id')
    result_df.insert(0, 'id', column_to_move)
    return result_df

def flatten_helper(data: list):
    master_relations, master_attributes, dict_columns = get_relations(pd.DataFrame(data))

    # if there are still leftover columns containing shallow dictionaries then we want to flatten those as well but only if we have flattened all the big relations first
    if len(dict_columns) != 0 and len(master_relations) == 0:
        dict_col = dict_columns[0]
        df = pd.json_normalize(data, meta=[dict_col])
        return flatten_helper(df.to_dict('records'))

    if len(master_relations) == 0:
        return pd.DataFrame(data)

    relation = master_relations[0]
    attributes = master_attributes.copy()

    # the resulting table columns should still include the columns that have yet to be flattened
    attributes.extend(master_relations[1:])

    df = pd.json_normalize(data, relation, attributes, record_prefix=f'{relation}.')
    df = filter_column_substrings(df)
    return flatten_helper(df.to_dict('records'))

def flatten_files(input_dir_path):
    # If the directory doesn't exist throw an error.
    if not input_dir_path.exists():
        print("torch does not detect an 'input' directory in the current directory.\ncannot flatten without 'input' directory.")
        return 

    # read every file in the directory path
    for file_path in directory_path.iterdir():
        if file_path.is_file():  # Ensure it's a file (not a directory)
            print(f"Reading file: {file_path}")
            with file_path.open("r") as f:
                print(f.read())
    

if __name__ == "__main__":
    # parse_program_args()
    target = 'Patient'

    data = read_ndjson(f'{target}.ndjson')
    df = flatten(data, target)
    csv_writer.save_csv_output(df, f'{target}.csv')

    print(df)
















