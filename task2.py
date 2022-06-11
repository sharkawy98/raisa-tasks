import pandas as pd



def check_code_validation(code: str) -> int:
    '''Create Python set (which remove duplicate) from code
    ### Example:
    - set('aabbccd') = {'a', 'b', 'c', 'd'}
    - len('aabbccd') - len((set('aabbccd'))) = 3
    - 3 > 2 = true => invalid => return 0
    '''
    if (len(code) - len(set(code))) > 2:
        return 0  # invalid
    else:
        return 1  # valid  


def task_2(wells_df: pd.DataFrame) -> pd.DataFrame:
    '''Task 2: Validate WellGroupCode column'''
    wells_df['IsValidCode'] = wells_df['WellGroupCode'].apply(check_code_validation)
    return wells_df
