from azure.storage.blob import BlobClient



def upload_file_to_blob(file_name: str) -> None:
    sas_url = f'https://raisademo2.blob.core.windows.net/raisa-task-solution/{file_name}?sp=acw&st=2022-05-31T17:54:22Z&se=2022-06-05T10:59:59Z&sv=2020-08-04&sr=c&sig=H0g%2BY%2FOnNIaYNVTsX%2FP42buWaowxIlxQDJ0xqH0gvqQ%3D'
    blob_client = BlobClient.from_blob_url(sas_url)

    with open(file_name, "rb") as data:
        blob_client.upload_blob(data)
    print(blob_client.blob_name)


def final_task() -> None:
    upload_file_to_blob('outputs/wells.csv')
    upload_file_to_blob('outputs/wells.py')
