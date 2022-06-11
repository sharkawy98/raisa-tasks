from azure.storage.blob import BlobClient
from time import time
import pandas as pd
from ast import literal_eval
from itertools import combinations



def download_blob_to_file(blob_name: str) -> None:
    sas_url = f'https://raisademo2.blob.core.windows.net/raisa-task/{blob_name}?sp=r&st=2022-05-31T17:49:47Z&se=2022-06-05T21:59:59Z&sv=2020-08-04&sr=c&sig=9M8ql9nYOhEYdmAOKUyetWbCU8hoWS72UFczkShdbeY%3D'

    blob_client = BlobClient.from_blob_url(sas_url)
    blob_data = blob_client.download_blob()

    local_file_name = blob_name.split('/')[1]
    with open(local_file_name, 'wb') as local_file:
        blob_data = blob_client.download_blob()
        blob_data.readinto(local_file)


def get_well_data_df() -> pd.DataFrame:
    well_data = pd.read_csv('well_data.csv')

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
    well_ranks = pd.read_json('well_ranks.json')

    # remove nonnumeric characters from WellIdentifier column then convert it to int
    well_ranks['WellIdentifier'] = well_ranks['WellIdentifier'].str.extract('(\d+)').astype('int')

    return well_ranks


def get_well_expenses_df() -> pd.DataFrame:
    well_expenses = None
    with open('well_expenses.txt') as f:
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


def get_roi_df() -> pd.DataFrame:
    roi = pd.read_csv('return_of_investment.csv')
    
    # parse comma separated string to Python list
    roi['monthly_worth'] = roi['monthly_worth'].str.split(', ')

    # convert monthly_worth list values to int
    roi['monthly_worth'] = roi['monthly_worth'].map(lambda x: list(map(int, x)))

    return roi


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


# create a list of all combinations of the 12 months indices
# i.e. (0,1), (0,2)... (4,5) ... (9,10), (9,11), (10,11)
time_pairs = list(combinations([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 2))

def return_of_investiment(monthly_worth: list) -> tuple: 
    time_pairs_roi = []
    for pair in time_pairs:
        buy_month, sell_month = pair
        roi = (monthly_worth[sell_month] - monthly_worth[buy_month]) / monthly_worth[buy_month]
        time_pairs_roi.append(roi)

    max_roi = max(time_pairs_roi)
    if max_roi < 0:  # if negative roi
        return (-1, -1)

    index_of_max_roi = time_pairs_roi.index(max_roi)
    return time_pairs[index_of_max_roi]


def upload_file_to_blob(file_name: str) -> None:
    sas_url = f'https://raisademo2.blob.core.windows.net/raisa-task-solution/{file_name}?sp=acw&st=2022-05-31T17:54:22Z&se=2022-06-05T10:59:59Z&sv=2020-08-04&sr=c&sig=H0g%2BY%2FOnNIaYNVTsX%2FP42buWaowxIlxQDJ0xqH0gvqQ%3D'
    blob_client = BlobClient.from_blob_url(sas_url)

    with open(file_name, "rb") as data:
        blob_client.upload_blob(data)
    print(blob_client.blob_name)



def task_0() -> None:
    '''Task 0: Load the data from Azure Blob Storage'''
    blobs = ['input/well_data.csv', 'input/well_ranks.json',
             'input/well_expenses.txt', 'input/return_of_investment.csv']
    for blob in blobs:
        download_blob_to_file(blob)

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

def task_2(wells_df: pd.DataFrame) -> pd.DataFrame:
    '''Task 2: Validate WellGroupCode column'''
    wells_df['IsValidCode'] = wells_df['WellGroupCode'].apply(check_code_validation)
    return wells_df

def task_3(wells_df: pd.DataFrame) -> pd.DataFrame:
    '''Task 3: Maximize the return on investment'''
    roi_df = get_roi_df()
    wells_df = pd.merge(wells_df, roi_df, on='WellId')
    
    time_pairs = wells_df['monthly_worth'].apply(return_of_investiment).tolist()
    wells_df[['OptimalBuyingMonth', 'OptimalSellingMonth']] = time_pairs

    wells_df = wells_df.drop(columns=['monthly_worth'])
    return wells_df

def final_task() -> None:
    upload_file_to_blob('sharkawyy98@gmailcom.csv')
    upload_file_to_blob('sharkawyy98@gmailcom.py')
	
if __name__ == "__main__":
	t1 = time()
	task_0()
	wells_df = task_1()
	wells_df = task_2(wells_df)
	wells_df = task_3(wells_df)
	wells_df.to_csv('sharkawyy98@gmailcom.csv', index=False)
	final_task()
	t2 = time()
	print(('It takes %.2f seconds to run all tasks') % (t2 - t1))
