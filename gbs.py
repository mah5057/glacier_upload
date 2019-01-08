import sys
import json
import boto3
import datetime
import traceback
from argparse import ArgumentParser
from multiprocessing import Process, Queue, current_process, RLock

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
VAULTS_COLLECTION = None

if config.has_key('dbUri'):
    DB_CLIENT = MongoClient(config['dbUri'])
    GLACIER_DB = DB_CLIENT['glacier']
    JOBS_COLLECTION = GLACIER_DB['jobs']
    ARCHIVES_COLLECTION = GLACIER_DB['archives']
    UPLOADS_COLLECTION = GLACIER_DB['uploads']
    VAULTS_COLLECTION = GLACIER_DB['vaults']
else:
    print "No database configuration found at %s, functionality will be limited..." % config_path

################################################################
# threading and helper methods
################################################################

def get_all_starting_byte_ranges(byte_ranges):
    return [ byte_range.get_starting_byte() for byte_range in byte_ranges ]

# partition byte ranges to distribute to workers
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

def upload_worker_process(lock, vault, byte_ranges, filename, upload_id, resume):
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
                    lock.acquire()
                    # Update remaining ranges
                    remaining_ranges_array = uploads_collection.find_one({"_id": upload_id})['incomplete_byte_ranges']
                    subdoc_id = byte_range.get_range_string()
                    remaining_ranges_array.remove(byte_range.get_starting_byte())
                    uploads_collection.update({"_id": upload_id}, 
                                            {"$set": 
                                                {"incomplete_byte_ranges": remaining_ranges_array}
                                                })
                    lock.release()

# TODO: => retrieve
# TODO: => logging

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
    list_archives_command_parser = subparsers.add_parser('list-archives')
    list_archives_command_parser.add_argument('-v', '--vault', type=str, default='',
                            help='Specify vault from which to display archives list')
    list_archives_command_parser.set_defaults(func=list_archives_command)

    # delete-archive commmand definition
    delete_archives_parser = subparsers.add_parser('delete-archive')
    delete_archives_parser.add_argument('-v', '--vault', type=str, default='',
                            help='Target glacier vault')
    delete_archives_parser.add_argument('-i', '--shortId', type=str, default='',
                            help='Target archive id')
    delete_archives_parser.set_defaults(func=delete_archive_command)

    # get-uploads command definition
    list_uploads_parser = subparsers.add_parser('list-uploads')
    list_uploads_parser.add_argument('-c', '--completed', action='store_true',
                        help='Get completed uploads')
    list_uploads_parser.set_defaults(func=list_uploads_command)

    # create-vault command
    create_vault_parser = subparsers.add_parser('create-vault')
    create_vault_parser.add_argument('name', metavar='N', type=str, nargs='+',
                        help='Name of new vault.')
    create_vault_parser.set_defaults(func=create_vault_command)

    # list-vaults command
    list_vaults_parser = subparsers.add_parser('list-vaults')
    list_vaults_parser.add_argument('-n', '--name', type=str, default='',
                        help='Find vaults with this name (exact match)')
    list_vaults_parser.set_defaults(func=list_vaults_command)

    # TODO: list-jobs command

    # TODO: retrieve-archive command

    arguments = parser.parse_args(args)
    arguments.func(arguments)

################################################################
# sub command methods
################################################################

#######################################
# create-vault command
#######################################
def create_vault_command(args):
    name = args.name

    if len(name) > 1:
        raise Exception("Too many arguments.")
    else:
        name = name[0]

    session = boto3.Session(profile_name='default')
    glacier_client = session.client('glacier')

    create_vault_response = glacier_client.create_vault(vaultName=name)

    if VAULTS_COLLECTION:
        vault_doc = {
            "vaultName": name,
            "createdOn": datetime.datetime.utcnow()
        }
        VAULTS_COLLECTION.insert(vault_doc)
    
    print "\nSuccessfully created vault '%s'\n" % name

#######################################
# list-vaults command
#######################################
def list_vaults_command(args):
    name = args.name

    header = "Name                    createdOn (UTC)"
    print "\n" + header
    print "-" * (len(header) + 20)
    if VAULTS_COLLECTION:
        rows = []

        if name:
            vaults = VAULTS_COLLECTION.find({"vaultName": name})
        else:
            vaults = VAULTS_COLLECTION.find()

        for vault in vaults:
            vault_name = vault['vaultName']
            date_created = vault['createdOn']

            date_created = date_created.strftime("%Y-%m-%d %H:%M")

            display_vn = vault_name[:20]
            display_date = date_created[:30]

            display_vn = display_vn + "..." if len(display_vn) < len(vault_name) else display_vn
            display_date = display_date + "..." if len(display_date) < len(date_created) else display_date

            row = "%s%s%s%s" % (display_vn, " " * (24 - len(display_vn)), display_date, " " * (34 - len(display_date)))

            rows.append(row)

        for row in rows:
            print row

        print "\n"

    else:
        raise Exception("DB REQUIRED")

#######################################
# list-uploads command
#######################################
def list_uploads_command(args):
    completed = args.completed

    header = "ID                    remaining parts            filename                completed"
    print "\n" + header
    print "-" * (len(header) + 20)
    if UPLOADS_COLLECTION:
        rows = []

        if completed:
            uploads = UPLOADS_COLLECTION.find({"completed": True})
        else:
            uploads = UPLOADS_COLLECTION.find({"completed": False})

        for upload in uploads:
            _id = upload["_id"]
            short_id = upload["shortId"]
            completed = str(upload["completed"])
            remaining_parts = str(len(upload["incomplete_byte_ranges"]))
            filename = upload["filename"].split('/')[-1]

            display_si = short_id[:18]
            display_rp = remaining_parts[:30]
            display_c = completed[:25]
            display_filename = filename[:20]

            display_si = display_si + "..." if len(display_si) < len(short_id) else display_si
            display_rp = display_rp + "..." if len(display_rp) < len(remaining_parts) else display_rp
            display_filename = display_filename + "..." if len(display_filename) < len(filename) else display_filename
            display_c = display_c + "..." if len(display_c) < len(completed) else display_c

            row =  "%s%s%s%s%s%s%s%s" % (display_si, " " * (22 - len(display_si)), 
                                        display_rp, " " * (27 - len(display_rp)), 
                                        display_filename, " " * (24 - len(display_filename)),
                                        display_c, " " * (29 - len(display_c)))
                
            rows.append(row)

        for row in rows:
            print row

        print "\n"

    else:
        raise Exception("DB REQUIRED")

#######################################
# list-archives command
#######################################
def list_archives_command(args):
    # TODO: make this more maintainable
    vault = args.vault

    header = "ID                    description                            vault"
    print "\n" + header
    print "-" * (len(header) + 20)
    if ARCHIVES_COLLECTION:
        rows = []
        
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

        print "\n"

    else:
        raise Exception("DB REQUIRED")

#######################################
# delete-archive command
#######################################
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

#######################################
# upload-archive command
#######################################
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

    print "\nPreparing file for upload..."

    f = GlacierUploadFile(file_path)

    if resume:
        if UPLOADS_COLLECTION:
            upload_2_resume = UPLOADS_COLLECTION.find_one({"shortId": resume})
            remaining_byte_ranges = upload_2_resume['incomplete_byte_ranges']
            remaining_ranges = get_remaining_byte_ranges(remaining_byte_ranges, f)
            partitioned_ranges = partition_byte_ranges(remaining_ranges, num_workers)
            upload_id = upload_2_resume["_id"]
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

            se_index = randint(1,15)
            short_upload_id = upload_id[se_index:se_index + 15]
            UPLOADS_COLLECTION.insert({
                "_id": upload_id,
                "shortId": short_upload_id,
                "incomplete_byte_ranges": all_starting_byte_ranges,
                "filename": file_path,
                "completed": False
            })

            print "Beginning upload %s...\n"  % short_upload_id
        else:
            print "Resuming upload %s...\n" % resume

        q = Queue()
        treehash_p = Process(target=calculate_treehash_worker_process, args=(f, q,))

        # start calculating that treehash
        treehash_p.start()
        
        upload_workers = []

        lock = RLock()
        # kick off uploader threads
        for set_of_ranges in partitioned_ranges:
            if set_of_ranges:
                p = Process(target=upload_worker_process, args=(lock, vault, set_of_ranges, file_path, upload_id, resume,))
                upload_workers.append(p)
                p.start()

        # wait for uploader and treehash calculator threads to finish
        for worker in upload_workers:
            worker.join()

        print "\nWaiting for checksum calculation..."
        treehash_p.join()

        # get treehash from the q
        treehash = q.get()

        print "\nCHECKSUM: %s" % treehash

        # complete multipart upload after upload parts and treehas calculator join
        print "\nCompleting multipart upload..."
        complete_mpu_response = glacier_client.complete_multipart_upload(accountId='-', 
                                                                        vaultName=vault, 
                                                                        uploadId=upload_id, 
                                                                        archiveSize=str(f.get_total_size_in_bytes()), 
                                                                        checksum=treehash)

        if ARCHIVES_COLLECTION:
            se_index = randint(1,15)
            # short id because the archive id from Glacier is too long for displaying
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
