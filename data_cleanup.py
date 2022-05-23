import pandas as pd

def rename_columns(df: pd.DataFrame, column_dict: dict):
    """Rename column names in dataframe
    Args:
        df => DataFrame to rename columns
        column_dict => Dictionary mapping of column names
    Returns:
        df => dataframe with renamed columns
    """
    df = df.rename(columns=column_dict)
    
    return df


def replace_all_values(df: pd.DataFrame, values: list, new_value: list):
    """Replace all values with another in a dataframe
    Args:
        values => The values to replace
        new_values => the values to replace with
        df => dataframe to load to db
    Returns:
        df => dataframe with replaced values
    """
    df = df.replace(to_replace=values, value=new_value)
    return df
    
    
def replace_specific_values(df: pd.DataFrame, values: dict):
    """Replace all values with another in a dataframe
    Args:
        values => The values to replace in dictionary format
        df => dataframe to load to db
    Returns:
        df => dataframe with replaced values
    """
    df = df.replace(to_replace=values)
    return df
    

if __name__ == '__main__':
    data = {'name': ['rice', 'beans', 'butter', 'milk', 'sugar'],
        'cost': [10, 10, 20, 30, 15]
        }

    df = pd.DataFrame(data)
    print(df)
    df = rename_columns(df, {'name': 'name_of_item'})
    print('-----------------renamed_columns------------------')
    print(df)
    df = replace_all_values(df, ['rice'], ['brown rice'])
    print('-----------------renamed_values------------------')
    print(df)
    df = replace_specific_values(df, {'name_of_item': {'beans': 'butter beans'}})
    print('-----------------renamed_specific_values------------------')
    print(df)



