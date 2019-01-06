import sys
import json
import boto3
import traceback
from argparse import ArgumentParser
from multiprocessing import Process, Queue, current_process

from random import randint
from os.path import expanduser
from pymongo import MongoClient
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
UPLOADS_COLLECTION = None

if config.has_key('dbUri'):
    DB_CLIENT = MongoClient(config['dbUri'])
    GLACIER_DB = DB_CLIENT['glacier']
    JOBS_COLLECTION = GLACIER_DB['jobs']
    ARCHIVES_COLLECTION = GLACIER_DB['archives']
    UPLOADS_COLLECTION = GLACIER_DB['uploads']
else:
    print "No database configuration found at %s, functionality will be limited..." % config_path

################################################################
# threading and helper methods
################################################################

# TODO: On resume, get uploaded parts and mark the byte range as uploaded (ByteRange.uploaded)

def get_all_starting_byte_ranges(byte_ranges):
    return [ byte_range.get_starting_byte() for byte_range in byte_ranges ]

# partition byte ranges to distribute to workers
# TODO: optimize number of workers?
def partition_byte_ranges(byte_ranges, num_workers):
    return [byte_ranges[x::num_workers] for x in xrange(num_workers)]

def get_remaining_byte_ranges(uploaded_byte_ranges, f):
    remaining_byte_ranges = []
    for byte_range in f.get_parts():
        if byte_range.get_starting_byte() in uploaded_byte_ranges:
            remaining_byte_ranges.append(byte_range)
    return remaining_byte_ranges

def get_long_id(short_id):
    archive_document = ARCHIVES_COLLECTION.find_one({"shortId": short_id})
    if archive_document:
        return archive_document["_id"]

def calculate_treehash_worker_process(upload_file, q):
    print "[%s] -- calculating treehash..." % current_process().name
    q.put(upload_file.get_treehash()) 

def upload_worker_process(vault, byte_ranges, filename, upload_id, resume):
    # each worker gets its own cnx to boto glacier
    session = boto3.Session(profile_name='default')
    glacier_client = session.client('glacier')
    upload_multipart_part = glacier_client.upload_multipart_part
    uploads_collection = None
    remaining_ranges_array = None

    if ARCHIVES_COLLECTION:
        # each worker get its own mongo cnx
        if config.has_key('dbUri'):
            db_client = MongoClient(config['dbUri'])
            glacier_db = db_client['glacier']
            uploads_collection = glacier_db['uploads']
            remaining_ranges_array = uploads_collection.find_one({"_id": upload_id})['incomplete_byte_ranges']
        else:
            raise Exception("DB REQUIRED")

    with open(filename, 'rb') as f:
        for byte_range in byte_ranges:
            print "[%s] -- uploading (%s)" % (current_process().name, byte_range.get_range_string())
            f.seek(byte_range.get_starting_byte())
            response = upload_multipart_part(accountId='-', 
                                            body=f.read(byte_range.get_chunk_size()), 
                                            range=byte_range.get_range_string(), 
                                            uploadId=upload_id, 
                                            vaultName=vault)

            if ARCHIVES_COLLECTION:
                if response['ResponseMetadata']['HTTPStatusCode'] in [200,202,204]:
                    subdoc_id = byte_range.get_range_string()
                    remaining_ranges_array.remove(byte_range.get_starting_byte())
                    uploads_collection.update({"_id": upload_id}, 
                                            {"$set": 
                                                {"incomplete_byte_ranges": remaining_ranges_array}
                                                })

# TODO: Need an arg parser 
# => retrieve
# => logging

################################################################
# main
################################################################

def main(args):
    """
    need argparse stuff for:
    gbs get-jobs --finished --in-progress
    gbs retrieve --vault "vaultname"? --archive-id "archiveId"
    argparse -- https://docs.python.org/dev/library/argparse.html#sub-commands
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()

    # TODO: resume upload logic
    # upload command definition
    upload_parser = subparsers.add_parser('upload-archive')
    upload_parser.add_argument('-v', '--vault', type=str, default='',
                    help='Target glacier vault')
    upload_parser.add_argument('-d', '--description', type=str, default='',
                    help='Description of archive')
    upload_parser.add_argument('-w', '--workers', type=int, default=8,
                    help='Maximum number of workers to deploy')
    upload_parser.add_argument('-r', '--resume', type=str, default=None,
                    help='Specify an upload id to resume that upload')
    upload_parser.add_argument('--dry-run', action='store_true',
                    help='Will only print byte ranges if specified')
    upload_parser.add_argument('filepath', metavar='F', type=str, nargs='+',
                    help='Path of file to upload')
    upload_parser.set_defaults(func=upload_archive_command)

    # list-archives command definition
    list_archives_parser = subparsers.add_parser('list-archives')
    list_archives_parser.add_argument('-v', '--vault', type=str, default='',
                            help='Specify vault from which to display archives list')
    list_archives_parser.set_defaults(func=list_archives)

    # delete-archive commmand
    delete_archives_parser = subparsers.add_parser('delete-archive')
    delete_archives_parser.add_argument('-v', '--vault', type=str, default='',
                            help='Target glacier vault')
    delete_archives_parser.add_argument('-i', '--shortId', type=str, default='',
                            help='Target archive id')
    delete_archives_parser.set_defaults(func=delete_archive_command)

    # get-uploads command
    get_uploads_parser = subparsers.add_parser('get-uploads')
    get_uploads_parser.add_argument('--completed', action='store_true',
                        help='Get completed uploads')
    get_uploads_parser.add_argument('-i', '--id', type=str, default=None,
                        help='Get upload with this id')
    get_uploads_parser.set_defaults(func=get_uploads_command)

    # TODO: get-vaults command

    # TODO: get-jobs command

    # TODO: retrieve command

    # TODO: create-vault command

    # TODO: get-long-id command
    
    arguments = parser.parse_args(args)
    arguments.func(arguments)

################################################################
# sub command methods
################################################################

def get_uploads_command(args):
    _id = args.id

    # do same as in list_archives

def list_archives(args):
    # TODO: make this more maintainable
    vault = args.vault

    header = "ID                    description                            vault"
    print header
    print "-" * (len(header) + 20)
    if ARCHIVES_COLLECTION:
        rows = []
        longest_row = len(header)
        
        if vault:
            archives = ARCHIVES_COLLECTION.find({"vault": vault,
                                                "deleted": {"$exists": False}})
        else:
            archives = ARCHIVES_COLLECTION.find({"deleted": {"$exists": False}})

        for archive in archives:
            short_id = archive["shortId"]
            description = archive["description"]
            vault = archive["vault"]

            display_si = short_id[:18]
            display_description = description[:34]
            display_vault = vault[:21]

            display_si = display_si + "..." if len(display_si) < len(short_id) else display_si
            display_description = display_description + "..." if len(display_description) < len(description) else description
            display_vault = display_vault + "..." if len(display_vault) < len(vault) else display_vault

            row =  "%s%s%s%s%s%s" % (display_si, " " * (22 - len(display_si)), 
                                     display_description, " " * (39 - len(display_description)), 
                                     display_vault, " " * (25 - len(display_vault)))
            
            rows.append(row)

        for row in rows:
            print row
    else:
        raise Exception("DB REQUIRED")

def delete_archive_command(args):
    vault = args.vault
    short_id = args.shortId

    _id = get_long_id(short_id)
    
    if not vault:
        if ARCHIVES_COLLECTION:
            vault = ARCHIVES_COLLECTION.find_one({"_id": _id})['vault']
        else:
            raise Exception("DB OR VAULT NAME REQUIRED: cannot determine vault without name.")

    session = boto3.Session(profile_name='default')
    glacier_client = session.client('glacier')

    delete_response = glacier_client.delete_archive(accountId='-',
                                                    archiveId=_id,
                                                    vaultName=vault,)

    ARCHIVES_COLLECTION.update_one({"_id": _id}, {"$set": {"deleted": True}}, upsert=False)

    if delete_response['ResponseMetadata']['HTTPStatusCode'] in [200, 202, 204]:
        print "\nSuccessfully deleted archive w/ id: %s from vault: %s!!\n" % (short_id, vault)
    else:
        print "\nFailed to delete archive, response:\n"
        print deleted_response

def upload_archive_command(args):
    vault = args.vault
    description = args.description
    num_workers = args.workers
    file_path = args.filepath
    dry_run = args.dry_run
    resume = args.resume

    if len(file_path) > 1:
        raise Exception("Too many arguments.")
    else:
        file_path = file_path[0]

    session = boto3.Session(profile_name='default')
    glacier_client = session.client('glacier')

    print "Preparing file for upload..."

    f = GlacierUploadFile(file_path)

    if resume:
        if UPLOADS_COLLECTION:
            remaining_byte_ranges = UPLOADS_COLLECTION.find_one({"_id": resume})['incomplete_byte_ranges']
            remaining_ranges = get_remaining_byte_ranges(remaining_byte_ranges, f)
            partitioned_ranges = partition_byte_ranges(remaining_ranges, num_workers)
            upload_id = resume
        else:
            raise Exception("DB REQUIRED")
    else:
        partitioned_ranges = partition_byte_ranges(f.get_parts(), num_workers)

    if not dry_run:
        print "Initializing multipart upload to Amazon Glacier...\n"

        if not resume:
            # initialize multipart upload
            init_mpu_response = glacier_client.initiate_multipart_upload(
                        accountId='-',
                        vaultName=vault,
                        archiveDescription=description,
                        partSize=str(f.get_part_size()),
                    )

            upload_id = init_mpu_response['uploadId']

            all_starting_byte_ranges = get_all_starting_byte_ranges(f.get_parts())

            UPLOADS_COLLECTION.insert({
                "_id": upload_id,
                "incomplete_byte_ranges": all_starting_byte_ranges,
                "completed": False
            })

        q = Queue()
        treehash_p = Process(target=calculate_treehash_worker_process, args=(f, q,))

        # start calculating that treehash
        treehash_p.start()
        
        upload_workers = []

        # kick off uploader threads
        for set_of_ranges in partitioned_ranges:
            if set_of_ranges:
                p = Process(target=upload_worker_process, args=(vault, set_of_ranges, file_path, upload_id, resume,))
                upload_workers.append(p)
                p.start()

        # wait for uploader and treehash calculator threads to finish
        for worker in upload_workers:
            worker.join()

        print "\nWaiting for treehash calculation..."
        treehash_p.join()

        # get treehash from the q
        treehash = q.get()

        print "\nTREEHASH: %s" % treehash

        # complete multipart upload after upload parts and treehas calculator join
        print "\nCompleting multipart upload..."
        complete_mpu_response = glacier_client.complete_multipart_upload(accountId='-', 
                                                                        vaultName=vault, 
                                                                        uploadId=upload_id, 
                                                                        archiveSize=str(f.get_total_size_in_bytes()), 
                                                                        checksum=treehash)

        if ARCHIVES_COLLECTION:
            se_index = randint(1,15)
            short_id = complete_mpu_response['archiveId'][se_index:se_index + 15]
            print "\nWriting document with ID: %s to database..." % short_id
            archive_doc = {
                "_id": complete_mpu_response['archiveId'],
                "shortId": short_id,
                "description": description,
                "vault": vault,
                "checksum": complete_mpu_response['checksum'],
                "location": complete_mpu_response['location']
            }
            ARCHIVES_COLLECTION.insert(archive_doc)
            UPLOADS_COLLECTION.update({"_id": upload_id}, {"$set": {"completed": True}})
            print "\nWritten to database: %s\n" % str(archive_doc)
        else:
            print "\nComplete response: %s\n" % str(complete_mpu_response)
    else:
        print "\nProcess to byte range mappings"
        print "------------------------------"
        for process, set_of_ranges in enumerate(partitioned_ranges, 1):
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
