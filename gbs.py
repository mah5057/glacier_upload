import sys
import json
import boto3
import traceback
from argparse import ArgumentParser
from multiprocessing import Process, Queue, current_process

from pymongo import MongoClient
from os.path import expanduser
from glacier_upload_file import GlacierUploadFile

"""
For readme later:
sample config:
{
    "dbUri": "mongodb://..."
}
to be placed at ~/.gbs/db_config
"""

# connect to the database if config exists
config_path = expanduser('~') + "/.gbs/db_config"
with open(config_path, 'r') as f:
    config = json.load(f)

DB_CLIENT = None
GLACIER_DB = None
JOBS_COLLECTION = None
ARCHIVES_COLLECTION = None

if config.has_key('dbUri'):
    DB_CLIENT = MongoClient(config['dbUri'])
    GLACIER_DB = DB_CLIENT['glacier']
    JOBS_COLLECTION = GLACIER_DB['jobs']
    ARCHIVES_COLLECTION = GLACIER_DB['archives']
else:
    print "No database configuration found at %s, functionality will be limited..." % config_path

################################################################
# threading and helper methods
################################################################

# TODO: On resume, get uploaded parts and mark the byte range as uploaded (ByteRange.uploaded)

# partition byte ranges to distribute to workers
# TODO: optimize number of workers?
def partition_byte_ranges(byte_ranges, num_workers):
    return [byte_ranges[x::num_workers] for x in xrange(num_workers)]

def calculate_treehash_worker_process(upload_file, q):
    print "[%s] -- calculating treehash..." % current_process().name
    q.put(upload_file.get_treehash()) 

def upload_worker_process(vault, byte_ranges, filename, upload_id):
    # each worker gets its own cnx to boto glacier
    session = boto3.Session(profile_name='default')
    glacier_client = session.client('glacier')
    upload_multipart_part = glacier_client.upload_multipart_part

    with open(filename, 'rb') as f:
        for byte_range in byte_ranges:
            print "[%s] -- uploading (%s)" % (current_process().name, byte_range.get_range_string())
            f.seek(byte_range.get_starting_byte())
            response = upload_multipart_part(accountId='-', 
                                            body=f.read(byte_range.get_chunk_size()), 
                                            range=byte_range.get_range_string(), 
                                            uploadId=upload_id, 
                                            vaultName=vault)

# TODO: Need an arg parser 
# => upload (resume local file?) (file, vault, archive_description, )
# => retrieve
# => main()
# => logging
# => config file
# => database

################################################################
# main
################################################################

def main(args):
    """
    need argparse stuff for:
    gbs upload --vault "vaultname" --description "archive description" <File>
    gbs get-jobs --finished --in-progress
    gbs retrieve --vault "vaultname"? --archive-id "archiveId"
    argparse -- https://docs.python.org/dev/library/argparse.html#sub-commands
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()

    # upload command definition
    ###########################
    upload_parser = subparsers.add_parser('upload')
    upload_parser.add_argument('-v', '--vault', type=str, default='',
                    help='Target glacier vault')
    upload_parser.add_argument('-d', '--description', type=str, default='',
                    help='Description of archive')
    upload_parser.add_argument('-w', '--workers', type=int, default=8,
                    help='Maximum number of workers to deploy')
    upload_parser.add_argument('--dry-run', action='store_true',
                    help='Will only print byte ranges if specified')
    upload_parser.add_argument('filepath', metavar='F', type=str, nargs='+',
                    help='Path of file to upload')
    upload_parser.set_defaults(func=upload_command)

    # TODO: delete-archive commmand

    # TODO: get-archives command

    # TODO: get-vaults command

    # TODO: get-jobs command

    # TODO: retrieve command

    # TODO: create-vault command
    
    arguments = parser.parse_args(args)
    arguments.func(arguments)

################################################################
# sub command methods
################################################################

def upload_command(args):
    # now parse arguments and execute upload logic
    vault = args.vault
    description = args.description
    num_workers = args.workers
    file_path = args.filepath
    dry_run = args.dry_run

    if len(file_path) > 1:
        raise Exception("Too many arguments.")
    else:
        file_path = file_path[0]

    session = boto3.Session(profile_name='default')
    glacier_client = session.client('glacier')

    print "Preparing file for upload..."

    f = GlacierUploadFile(file_path)
    # divide byte ranges to upload for the workers
    divided_ranges = partition_byte_ranges(f.get_parts(), num_workers)

    if not dry_run:
        print "Initializing multipart upload to Amazon Glacier..."

        # initialize multipart upload
        init_mpu_response = glacier_client.initiate_multipart_upload(
                    accountId='-',
                    vaultName=vault,
                    archiveDescription='testing upload 6 GiB',
                    partSize=str(f.get_part_size()),
                )

        upload_id = init_mpu_response['uploadId']

        q = Queue()
        treehash_p = Process(target=calculate_treehash_worker_process, args=(f, q,))

        # start calculating that treehash
        treehash_p.start()
        
        upload_workers = []
        # kick off uploader threads
        for set_of_ranges in divided_ranges:
            if set_of_ranges:
                p = Process(target=upload_worker_process, args=(vault, set_of_ranges, file_path, upload_id,))
                upload_workers.append(p)
                p.start()

        # wait for uploader and treehash calculator threads to finish
        for worker in upload_workers:
            worker.join()

        print "Waiting for treehash calculation..."
        treehash_p.join()

        # get treehash from the q
        treehash = q.get()

        print treehash

        # complete multipart upload after upload parts and treehas calculator join
        print "Completing multipart upload..."
        complete_mpu_response = glacier_client.complete_multipart_upload(accountId='-', 
                                                                        vaultName=vault, 
                                                                        uploadId=upload_id, 
                                                                        archiveSize=str(f.get_total_size_in_bytes()), 
                                                                        checksum=treehash)

        if ARCHIVES_COLLECTION:
            print "Saving to database..."
            archive_doc = {
                "_id": complete_mpu_response['archiveId'],
                "description": description,
                "vault": vault,
                "checksum": complete_mpu_response['checksum'],
                "location": complete_mpu_response['location']
            }
            ARCHIVES_COLLECTION.insert(archive_doc)
            print "\nInserted to database: " + str(archive_doc)
        else:
            print "\nComplete response: " + str(complete_mpu_response)
    else:
        print "\nProcess to byte range mappings"
        print "------------------------------"
        for process, set_of_ranges in enumerate(divided_ranges, 1):
            if set_of_ranges:
                print "\nProcess %d" % process
                for one_range in set_of_ranges:
                    print "    %s" % one_range.get_range_string()

################################################################
# run main with sys args
################################################################
if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except SystemExit, e:
        if e.code == 0:
            pass
    except:
        traceback.print_exc()
