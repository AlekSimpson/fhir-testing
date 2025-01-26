import pandas as pd

PATH = -1
META = 0
NEST = 1

def save_csv_output(df, filename):
    if df is None:
        return

    df.to_csv(f"output/{filename}")

def read_ndjson(filename):
    return pd.read_json(f"input/{filename}", lines=True).to_dict('records')

def does_list_have_list(array: list):
    return any(isinstance(x, list) for x in array)

def does_list_have_dict(array: list):
    return any(isinstance(x, dict) for x in array)

def is_nested_relation(data: dict, attribute: str):
    element = data[attribute]

    if not isinstance(element, dict) and not isinstance(element, list):
        return META

    if isinstance(element, list):
        if len(element) <= 3 and all(isinstance(el, dict) for el in element):
            if all(all(is_nested_relation(obj, key) == META for key in obj.keys()) for obj in element):
                return PATH

        is_list_of_lists = all(isinstance(item, list) for item in element)
        is_list_of_objs = all(isinstance(item, dict) for item in element)
        if not is_list_of_lists and not is_list_of_objs:
            return META
        return NEST
    
    if isinstance(element, dict):
        if len(element.keys()) >= 3:
            return NEST
        if all(is_nested_relation(element, key) == META for key in element.keys()):
            return META
        if all(is_nested_relation(element, key) >= PATH for key in element.keys()):
            return NEST
    return NEST

def unfold_shallow_nested_attributes(data: dict, meta: list):
    new_list = []
    for entry in meta:
        if not isinstance(data[entry], dict):
            new_list.append(entry)
            continue

        for key, value in data[entry].items():
            new_list.append([entry, key])
    return new_list
































