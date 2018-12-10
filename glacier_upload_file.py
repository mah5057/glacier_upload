import os
import sys
import math

from botocore.utils import calculate_tree_hash
from glacier_upload_file_part import GlacierUploadFilePart

MiB = 1024 ** 2
GiB = MiB * 1024

class GlacierUploadFile():

    def __init__(self, filename):
        self.filename = filename
        self.parts = []
        self.treehash = None
        self.part_size = 0
        self.total_size_in_bytes = 0
        self._compute_parts()

    def get_part_size(self):
        return str(self.part_size)

    def get_treehash(self):
        return self.treehash

    def get_parts(self):
        return self.parts

    def get_total_size_in_bytes(self):
        return str(self.total_size_in_bytes)

    def _compute_parts(self):
        file_size_in_bytes = os.path.getsize(self.filename)
        self.total_size_in_bytes = file_size_in_bytes
        if file_size_in_bytes > GiB:
            self.part_size = GiB
            self._do_compute_parts(file_size_in_bytes, GiB)
        else:
            self.part_size = MiB
            self._do_compute_parts(file_size_in_bytes, MiB)

    def _do_compute_parts(self, file_size_in_bytes, chunk_size_in_bytes):

        number_of_chunks = file_size_in_bytes / chunk_size_in_bytes
        final_chunk = file_size_in_bytes % chunk_size_in_bytes
        print "File size: %s" % str(file_size_in_bytes)
        print "Chunk size: %s" % str(chunk_size_in_bytes)
        print "Final chunk size: %s" % str(final_chunk)
        print "Number of chunks: %s" % str(number_of_chunks)

        with open(self.filename, 'rb') as f:

            # compute treehash using botocore.utils
            self.treehash = calculate_tree_hash(f)

            start_of_range = 0

            # the above calculate tree hash reads the file in 1 MiB chunks,
            # so we need to seek back to byte 0
            f.seek(0)
            for x in range(0, number_of_chunks):
                print "Chunk: %s" % str(x)
                byte_range = "bytes %s-%s/%s" % (start_of_range, start_of_range + chunk_size_in_bytes - 1, file_size_in_bytes)
                print byte_range
                body = f.read(chunk_size_in_bytes) 
                self.parts.append(GlacierUploadFilePart(body, byte_range))
                start_of_range += chunk_size_in_bytes

            if final_chunk:
                byte_range = "bytes %s-%s/%s" % (start_of_range, start_of_range + final_chunk - 1, file_size_in_bytes)
                print byte_range
                body = f.read(final_chunk)
                self.parts.append(GlacierUploadFilePart(body, byte_range))

            if not f.read() == "":
                raise Exception("EOF not reached with calculated byte ranges.")

