import sys
import boto3
import traceback
from argparse import ArgumentParser
from multiprocessing import Process, Queue, current_process

from glacier_upload_file import GlacierUploadFile

VAULT='examplevault'

# client to initialize glacier
session = boto3.Session(profile_name='default')
glacier_client = session.client('glacier')

################################################################
# threading and helper methods
################################################################

# TODO: On resume, get uploaded parts and mark the byte range as uploaded (ByteRange.uploaded)

# get a list of lists of the ByteRanges that comprise the file to divide and share amongst the workers
# TODO: optimize number of workers?
def divvy_byte_ranges(byte_ranges, num_workers):
    return [byte_ranges[x::num_workers] for x in xrange(num_workers)]

def calculate_treehash_worker_process(upload_file, q):
    print "[%s] -- calculating treehash..." % current_process().name
    q.put(upload_file.get_treehash()) 

def upload_worker_process(byte_ranges, filename, upload_id):
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
                                            vaultName=VAULT)

# TODO: Need an arg parser 
# => upload (resume local file?) (file, vault, archive_description, )
# => retrieve
# => main()

################################################################
# main
################################################################

def main(args):
    """
    need argparse stuff for:
    rgbs upload --vault "vaultname" --description "archive description" <File>
    rgbs get-jobs --finished --in-progress
    rgbs retrieve --vault "vaultname"? --archive-id "archiveId"
    argparse -- https://docs.python.org/dev/library/argparse.html#sub-commands
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()

    # upload command definition
    ###########################
    upload_parser = subparsers.add_parser('upload')
    upload_parser.add_argument('-v', '--vault', type=str, default='')
    upload_parser.add_argument('-d', '--description', type=str, default='')
    parser.add_argument('filepath', metavar='F', type=str, nargs='+',
                    help='path to file to upload')
    upload_parser.set_defaults(func=upload_command)

    # TODO: get-jobs command

    # TODO: retrieve command
    
    arguments = parser.parse_args(args)
    arguments.func(arguments)

################################################################
# sub command methods
################################################################

def upload_command(args):
    # now parse arguments and execute upload logic
    print args

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
"""
number_of_workers = 8

file_path = sys.argv[1]

print "Preparing file for upload..."

f = GlacierUploadFile(file_path)
# divide byte ranges to upload for the workers
divided_ranges = divvy_byte_ranges(f.get_parts(), number_of_workers)

print "Initializing multipart upload to Amazon Glacier..."

# initialize multipart upload
init_mpu_response = glacier_client.initiate_multipart_upload(
            accountId='-',
            vaultName=VAULT,
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
    p = Process(target=upload_worker_process, args=(set_of_ranges, file_path, upload_id,))
    upload_workers.append(p)
    p.start()

# wait for uploader and treehash calculator threads to finish
for worker in upload_workers:
    worker.join()

print "Waiting for treehash calculation..."
treehash_p.join()

# get treehash from the q
treehash = q.get()

# complete multipart upload after upload parts and treehas calculator join
print "Completing multipart upload..."
complete_mpu_response = glacier_client.complete_multipart_upload(accountId='-', 
                                                                 vaultName=VAULT, 
                                                                 uploadId=upload_id, 
                                                                 archiveSize=str(f.get_total_size_in_bytes()), 
                                                                 checksum=treehash)

print "\nComplete response: " + str(complete_mpu_response)
"""


