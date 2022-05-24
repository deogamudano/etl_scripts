import pandas as pd
import json
import os


def normalise_json(column: str, file_path: str = '', encoding: str = '', df: pd.DataFrame = pd.DataFrame()):
    """Normalize data from a json file
    Args:
        file_path => Path to file containing the json to be extracted
        encoding => File encoding
        column => Column to normalize
        df => DataFrame with nested json to normalize
        df => DataFrame with nested json to normalize
        
    Returns:
        df => Normalised DataFrame
        data => Original DataFrame 
    NOTE: 
        You can follow the principle of the steps to further normalise
        other columns
        You can also use the method with just the first and last argument
        for a table with a nested json column
    """
    try:
        data = json.load(open(file_path, encoding=encoding))
        print(data)
        df = pd.json_normalize(data, sep='_')
        print(df.explode(column)[column])
        try:
            df = pd.concat([df.explode(column).drop([column], axis=1),
                       df.explode(column)[column].apply(pd.Series).add_prefix('{}_'.format(column))],
                      axis=1).reset_index()
        except:
            df = pd.concat([df.explode(column).drop([column], axis=1),
                       df.explode(column)[column].apply(pd.Series).add_prefix('{}_'.format(column))],
                      axis=1).reset_index()
        return df, data
    except:
        data = df
        print(df.columns)
        print(data)
        df = pd.concat([df.explode(column).drop([column], axis=1),
                   df.explode(column)[column].apply(pd.Series).add_prefix('{}_'.format(column))],
                  axis=1).reset_index()
        return df, data
        
    
    


if __name__ == '__main__':
    #Create test json file
    test_json = { 
        "coffee": {
            "region": [
                {"id":1,"name":"John Doe"},
                {"id":2,"name":"Don Joeh"}
            ],
            "country": {"id":2,"company":"ACME"}
        }, 
        "brewing": {
            "region": [
                {"id":1,"name":"John Doe"},
                {"id":2,"name":"Don Joeh"}
            ],
            "country": {"id":2,"company":"ACME"}
        }
    }
    #save test json file
    with open('test_json.json', 'w') as fp:
        json.dump(test_json, fp)
    #normalise json file that was saved
    df, data = normalise_json(file_path = 'test_json.json', encoding='utf8', column='coffee_region')
    print('--------------dataframe before normalisation-----------')
    print(data)
    print('--------------dataframe after normalisation-----------')
    print(df)
    #normalise a dataframe with nested json column
    df2, data2 = normalise_json(df=pd.json_normalize(test_json, sep = '_'), column='coffee_region')
    print('--------------dataframe before normalisation-----------')
    print(data2)
    print('--------------dataframe after normalisation-----------')
    print(df2)
    #normalise a dataframe with nested json column
    df3, data3 = normalise_json(df=df2, column='brewing_region')
    print('--------------dataframe before normalisation-----------')
    print(data3)
    print('--------------dataframe after normalisation-----------')
    print(df3)
    #close file connection
    fp.close()
    #delete json test file if exists
    if os.path.exists("test_json.json"):
      os.remove("test_json.json")
    else:
      print("The file does not exist")
    
    
