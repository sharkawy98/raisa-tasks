import pandas as pd
from ast import literal_eval



def get_well_data_df() -> pd.DataFrame:
    well_data = pd.read_csv('inputs/well_data.csv')

    # convert json string into Python object (i.e dict)
    well_data['WellValues'] = well_data['WellValues'].apply(literal_eval)

    # create DataFrame of multiple column values from WellValues dict
    well_values = pd.DataFrame(well_data['WellValues'].values.tolist())
    well_values = well_values.astype(int)

    # concatenate well_data with well_values row by row
    well_data = pd.concat([well_data, well_values], axis=1)

    well_data = well_data.drop(columns=['WellValues'])
    return well_data


def get_well_ranks_df() -> pd.DataFrame:
    well_ranks = pd.read_json('inputs/well_ranks.json')

    # remove nonnumeric characters from WellIdentifier column then convert it to int
    well_ranks['WellIdentifier'] = well_ranks['WellIdentifier'].str.extract('(\d+)').astype('int')

    return well_ranks


def get_well_expenses_df() -> pd.DataFrame:
    well_expenses = None
    with open('inputs/well_expenses.txt') as f:
        well_expenses = f.read()
        # add '-' as a separtor between well_expenses records
        well_expenses = well_expenses.replace(' WellId', '-WellId')
        
        # split whole string (by '-') to make multiple rows
        # split each row (by ' ') to make multiple columns
        well_expenses = map(str.split, well_expenses.split('-'))

    expenses_list = []
    for expense_record in well_expenses:
        expense_dict = {
            'WellId': 0,
            'WellExpensesA': 0,
            'WellExpensesB': 0,
            'WellExpensesC': 0
        }
        
        for column in expense_record:
            # split each column name from its value
            # eg. WellId:3050 -> key=WellId, value=3050
            key, value = column.split(':')
            expense_dict[key] = int(value)
        expenses_list.append(expense_dict)

    well_expenses = pd.DataFrame(expenses_list)
    return well_expenses


def task_1() -> pd.DataFrame:
    '''Task 1: Create wells dataframe'''
    well_data = get_well_data_df()
    well_ranks = get_well_ranks_df()
    well_expenses = get_well_expenses_df()
    
    # join wells_data, well_ranks and well_expenses
    wells_df1 = pd.merge(well_data, well_expenses, on='WellId')
    wells_df2 = pd.merge(wells_df1, well_ranks, left_on='WellId', right_on='WellIdentifier')
    
    # select the needed columns only
    wells_df = wells_df2[['WellId', 'WellValueA', 'WellValueB', 'WellValueC', 
                         'WellExpensesA', 'WellExpensesB', 'WellExpensesC', 
                         'WellRankA', 'WellRankB', 'WellRankC', 'WellGroupCode']]
    return wells_df
