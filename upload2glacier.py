import sys
import boto3

from glacier_upload_file import GlacierUploadFile

VAULT='examplevault'

session = boto3.Session(profile_name='default')
glacier_client = session.client('glacier')

file_path = sys.argv[1]

print "Preparing file for upload..."

f = GlacierUploadFile(file_path)

print "Initializing multipart upload to Amazon Glacier..."

# initialize multipart upload
init_mpu_response = glacier_client.initiate_multipart_upload(
            accountId='-',
            vaultName=VAULT,
            archiveDescription='testing upload 6 GiB',
            partSize=f.get_part_size(),
        )

upload_id = init_mpu_response['uploadId']

print "Uploading parts..."

partno = 1
for part in f.get_parts():
    print "Part #%s" % str(partno)
    print "size: %s" % str(part.get_size())
    response = glacier_client.upload_multipart_part(accountId='-', body=part.get_body(), range=part.get_byte_range(), uploadId=upload_id, vaultName=VAULT)
    partno += 1

print "Completing multipart upload..."

# complete multipart upload
complete_mpu_response = glacier_client.complete_multipart_upload(accountId='-', vaultName=VAULT, uploadId=upload_id, archiveSize=f.get_total_size_in_bytes(), checksum=f.get_treehash())

print "\nComplete response: " + str(complete_mpu_response)

