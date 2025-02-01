import os
import re
from pathlib import Path
import pandas as pd

class CSVWriter:
    def __init__(self):
        self.current_bundle_directory = None

    def save_csv_output(self, df, filename):
        if df is None:
            return
    
        if self.current_bundle_directory == None:
            df.to_csv(Path(f"output/{filename}"))
        else:
            try:
                os.mkdir(Path(f'output/{self.current_bundle_directory}'))
            except:
                pass

            df.to_csv(Path(f'output/{self.current_bundle_directory}/{filename}'))

def read_ndjson(filename):
    return pd.read_json(Path(f"input/{filename}"), lines=True).to_dict('records')

def get_relations(df):
    """
    Returns a set of column names where at least one element is a Python list, as well as the columns that contain at least one dict or none.
    """
    assert type(df) == pd.DataFrame

    relation_columns = []
    dict_columns = []
    attribute_columns = []
    for col in df.columns:
        # Check if column is object dtype (lists are stored as objects)
        if df[col].dtype == 'object':
            # Check if any element in the column is a list
            if any(isinstance(x, list) for x in df[col]):
                relation_columns.append(col)
            elif any(isinstance(x, dict) for x in df[col]):
                dict_columns.append(col)
                attribute_columns.append(col)
            else:
                attribute_columns.append(col)
            continue

        attribute_columns.append(col)

    return relation_columns, attribute_columns, dict_columns

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

def depth_probe(obj, current_depth=0) -> int:
    """
    Calculate the maximum depth of nested lists/dictionaries.
    
    Args:
        obj: The input object (list, dict, or other).
        current_depth: Current depth level (used internally for recursion).
        
    Returns:
        Maximum depth of nested structures.
    """
    if not isinstance(obj, (list, dict)):
        return current_depth
    
    max_depth = current_depth + 1
    
    if isinstance(obj, list):
        for item in obj:
            max_depth = max(max_depth, depth_probe(item, current_depth + 1))
    else:  # Handle dictionaries
        for value in obj.values():
            max_depth = max(max_depth, depth_probe(value, current_depth + 1))
    
    return max_depth

def width_probe(obj, current_width = 0):
    """
    Calculate the maximum width of nested lists/dictionaries.
    
    Args:
        obj: The input object (list, dict, or other).
        current_width: Current width level (used internally for recursion).
        
    Returns:
        Maximum width of nested structures.
    """
    if not isinstance(obj, (list, dict)):
        return current_width 

    if isinstance(obj, list):
        max_width = len(obj)
        for item in obj:
            max_width = max(max_width, width_probe(item, max_width))
    else:
        max_width = len(obj.values())
        for value in obj.values():
            max_width = max(max_width, width_probe(value, max_width))

    return max_width

def filter_column_substrings(df):
    """
    Filters out substrings matching the regex pattern `.N*` from column names,
    where N is any single-digit character (0-9).

    Parameters:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: A new DataFrame with filtered column names.
    """
    # Define the regex pattern to match `.N*` where N is a single digit
    pattern = re.compile(r'\.\d+')
    
    # Apply the regex to each column name and replace matches with an empty string
    new_columns = [pattern.sub('', col) for col in df.columns]
    
    # Create a copy of the DataFrame to avoid modifying the original
    new_df = df.copy()
    new_df.columns = new_columns
    
    return new_df

























