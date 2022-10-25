import os
import re
import argparse
import glob
import sys
import shipyard_utils as shipyard
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core import exceptions
try:
    import exit_codes as ec
except BaseException:
    from . import exit_codes as ec


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--container-name',
        dest='container_name',
        required=True)
    parser.add_argument('--source-file-name-match-type',
                        dest='source_file_name_match_type',
                        default='exact_match',
                        choices={
                            'exact_match',
                            'regex_match'},
                        required=False)
    parser.add_argument(
        '--source-file-name',
        dest='source_file_name',
        required=True)
    parser.add_argument(
        '--source-folder-name',
        dest='source_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default=None,
        required=False)
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
    Azure Blob objects
    """
    container = ContainerClient.from_connection_string(
        conn_str=conn_str, container_name=container_name)
    return list(container.list_blobs(prefix=prefix))


def find_matching_files(file_blobs, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for blob in file_blobs:
        if re.search(file_name_re, blob.name):
            matching_file_names.append(blob.name)

    return matching_file_names


def azure_move_blob(
        connection_string,
        container_name,
        source_full_path,
        destination_full_path):
    """
    Moves a single blob inside the same Azure Storage Blob Container.
    Since there's no move funtion, this function is a combination of copy and delete
    """
    try:
        blob_client = BlobServiceClient.from_connection_string(conn_str=connection_string,
                                             container_name=container_name)
    except:
        print("Incorrect credentials")
        sys.exit(ec.EXIT_CODE_INCORRECT_CREDENTIALS)
    # get source blob url and copy to destination blob path
    source_blob = blob_client.get_blob_client(container_name, source_full_path)
    destination_blob = blob_client.get_blob_client(container_name, destination_full_path)

    destination_blob.start_copy_from_url(source_blob.url, requires_sync=True)
    copy_status_data = destination_blob.get_blob_properties().copy

    if copy_status_data.status != "success":
        # if copy failsm abort copy and return error message
        destination_blob.abort_copy(copy_status_data.id)
        print(f"Copy blob from {source_full_path} failed with status {copy_status_data.status}")
        sys.exit(ec.EXIT_CODE_AZURE_MOVE_ERROR)
    # successfully copied, so we delete the source path
    source_blob.delete_blob()



def main():
    args = get_args()
    set_environment_variables(args)
    connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    container_name = args.container_name
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = shipyard.files.combine_folder_and_file_name(
        source_folder_name, source_file_name)
    destination_folder_name = shipyard.files.clean_folder_name(args.destination_folder_name)
    source_file_name_match_type = args.source_file_name_match_type
    destination_file_name = args.destination_file_name

    if source_file_name_match_type == 'regex_match':
        file_names = find_azure_storage_blob_file_names(connection_string, container_name, prefix=source_folder_name)
        matching_file_names = find_matching_files(
            file_names, re.compile(source_file_name))
        if len(matching_file_names) == 0:
            print(f"No files matching {source_file_name} found")
            sys.exit(ec.EXIT_CODE_NO_MATCHES_FOUND)
        print(f'{len(matching_file_names)} files found. Preparing to move...')
        for index, key_name in enumerate(matching_file_names,1):
            dest_file_name = shipyard.files.determine_destination_file_name(source_full_path = key_name,destination_file_name = None)
            destination_full_path = shipyard.files.determine_destination_full_path(
                destination_folder_name = destination_folder_name,
                destination_file_name = dest_file_name,
                source_full_path = key_name,
                file_number = index
            )
            # destination_full_path = shipyard.files.combine_folder_and_file_name(destination_folder_name, key_name)
            print(f'Moving file {index} of {len(matching_file_names)}')
            azure_move_blob(
                source_full_path=key_name,
                destination_full_path=destination_full_path,
                container_name=container_name,
                connection_string=connection_string
            )
    else:
        destination_full_path = shipyard.files.determine_destination_full_path(
            destination_folder_name = destination_folder_name,
            destination_file_name = destination_file_name,
            source_full_path = source_full_path
        )
        # destination_full_path = shipyard.files.combine_folder_and_file_name(
        #     destination_folder_name, args.destination_file_name
        # )
        azure_move_blob(
                source_full_path=source_full_path,
                destination_full_path=destination_full_path,
                container_name=container_name,
                connection_string=connection_string
        )


if __name__ == '__main__':
    main()
