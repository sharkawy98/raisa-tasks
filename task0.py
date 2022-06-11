from azure.storage.blob import BlobClient



def download_blob_to_file(blob_name: str) -> None:
    sas_url = f'https://raisademo2.blob.core.windows.net/raisa-task/{blob_name}?sp=r&st=2022-05-31T17:49:47Z&se=2022-06-05T21:59:59Z&sv=2020-08-04&sr=c&sig=9M8ql9nYOhEYdmAOKUyetWbCU8hoWS72UFczkShdbeY%3D'

    blob_client = BlobClient.from_blob_url(sas_url)
    blob_data = blob_client.download_blob()

    local_file_name = blob_name.split('/')[1]
    with open(local_file_name, 'wb') as local_file:
        blob_data = blob_client.download_blob()
        blob_data.readinto(f'inputs/{local_file}')


def task_0() -> None:
    '''Task 0: Load the data from Azure Blob Storage'''
    blobs = ['input/well_data.csv', 'input/well_ranks.json',
             'input/well_expenses.txt', 'input/return_of_investment.csv']
    for blob in blobs:
        download_blob_to_file(blob)
