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

        with open(self.filename, 'rb') as f:
            number_of_chunks = file_size_in_bytes / chunk_size_in_bytes
            final_chunk = file_size_in_bytes % chunk_size_in_bytes

            # compute treehash using botocore.utils
            self.treehash = calculate_tree_hash(f)

            # create array of GlacierUploadFilePart(s)
            start_of_range = 0
            f.seek(0)
            for x in range(0, number_of_chunks):
                byte_range = "bytes %s-%s/*" % (start_of_range, start_of_range + chunk_size_in_bytes - 1)
                body = f.read(chunk_size_in_bytes) 
                self.parts.append(GlacierUploadFilePart(body, byte_range))
                start_of_range += chunk_size_in_bytes

            if final_chunk:
                byte_range = "bytes %s-%s/*" % (start_of_range, start_of_range + final_chunk)
                body = f.read(final_chunk)
                self.parts.append(GlacierUploadFilePart(body, byte_range))

            if not f.read() == "":
                raise Exception("EOF not reached with calculated byte ranges.")

