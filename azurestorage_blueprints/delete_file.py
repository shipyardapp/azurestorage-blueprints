import os
import sys
import re
import argparse
import shipyard_utils as shipyard
from azure.storage.blob import BlobClient, ContainerClient
from azure.core import exceptions


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--container-name',
        dest='container_name',
        required=True)
    parser.add_argument(
        '--source-file-name-match-type',
        dest='source_file_name_match_type',
        default='exact_match',
        choices={
            'exact_match',
            'regex_match'},
        required=False)
    parser.add_argument(
        '--source-folder-name',
        dest='source_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--source-file-name',
        dest='source_file_name',
        required=True)
    parser.add_argument(
        '--connection-string',
        dest='connection_string',
        default=None,
        required=True)
    return parser.parse_args()


def set_environment_variables(args):
    """
    Set Azure Connection String as environment variable if it's provided via keyword arguments
    rather than seeded as environment variables. This will override system defaults.
    """

    if args.connection_string:
        os.environ['AZURE_STORAGE_CONNECTION_STRING'] = args.connection_string
    return



def find_azure_storage_blob_file_names(conn_str, container_name, prefix=''):
    """
    Fetched all the files in the bucket which are returned in a list as
    Google Blob objects
    """
    container = ContainerClient.from_connection_string(
        conn_str=conn_str, container_name=container_name)
    return list(container.list_blobs(prefix=prefix))


def azure_find_matching_files(file_blobs, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for blob in file_blobs:
        if re.search(file_name_re, blob.name):
            matching_file_names.append(blob.name)

    return matching_file_names


def delete_azure_storage_blob_file(
        file_name,
        container_name,
        connection_string,
    ):
    """
    Delete Blob from Azure cloud storage
    """
    blob = BlobClient.from_connection_string(
        conn_str=connection_string,
        container_name=container_name,
        blob_name=file_name)

    blob.delete_blob()
    print(f'{container_name}/{file_name} delete function successfully ran')

    return


def main():
    args = get_args()
    set_environment_variables(args)
    connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    container_name = args.container_name
    source_file_name = args.source_file_name
    source_folder_name = shipyard.files.clean_folder_name(args.source_folder_name)
    source_full_path = shipyard.files.combine_folder_and_file_name(
        folder_name=source_folder_name, file_name=source_file_name)
    source_file_name_match_type = args.source_file_name_match_type

    if source_file_name_match_type == 'regex_match':
        file_names = find_azure_storage_blob_file_names(
            conn_str=connection_string,
            container_name=container_name,
            prefix=source_folder_name)
        matching_file_names = azure_find_matching_files(file_names,
                                                  re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing to delete...')
        if len(matching_file_names) == 0:
            print("No file matches found")
            sys.exit()

        for index, file_name in enumerate(matching_file_names):
            print(f'Deleting file {index+1} of {len(matching_file_names)}')
            delete_azure_storage_blob_file(
                file_name=file_name,
                container_name=container_name,
                connection_string=connection_string,
            )
    else:
        delete_azure_storage_blob_file(
            file_name=source_full_path,
            container_name=container_name,
            connection_string=connection_string,
        )


if __name__ == '__main__':
    main()
