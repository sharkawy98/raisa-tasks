import pandas as pd
from itertools import combinations



def get_roi_df() -> pd.DataFrame:
    roi = pd.read_csv('inputs/return_of_investment.csv')
    
    # parse comma separated string to Python list
    roi['monthly_worth'] = roi['monthly_worth'].str.split(', ')

    # convert monthly_worth list values to int
    roi['monthly_worth'] = roi['monthly_worth'].map(lambda x: list(map(int, x)))

    return roi


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


def task_3(wells_df: pd.DataFrame) -> pd.DataFrame:
    '''Task 3: Maximize the return on investment'''
    roi_df = get_roi_df()
    wells_df = pd.merge(wells_df, roi_df, on='WellId')
    
    time_pairs = wells_df['monthly_worth'].apply(return_of_investiment).tolist()
    wells_df[['OptimalBuyingMonth', 'OptimalSellingMonth']] = time_pairs

    wells_df = wells_df.drop(columns=['monthly_worth'])
    return wells_df
