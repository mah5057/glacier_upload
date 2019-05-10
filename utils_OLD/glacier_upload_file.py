import os
import sys
import math

from botocore.utils import calculate_tree_hash
from byte_range import ByteRange

MiB = 1024 ** 2
GiB = MiB * 1024

class GlacierUploadFile():

    def __init__(self, filename, custom_chunk_size=None):
        self.filename = filename
        self.parts = []
        self.part_size = 0
        self.total_size_in_bytes = 0
        self.custom_chunk_size = custom_chunk_size
        self._compute_byte_ranges()

    def get_part_size(self):
        return self.part_size

    def get_treehash(self):
        treehash = ''
        with open(self.filename, 'rb') as f:
            treehash = calculate_tree_hash(f)
        return treehash

    def get_parts(self):
        return self.parts
    
    def get_number_of_parts(self):
        return len(self.parts)

    def get_total_size_in_bytes(self):
        return self.total_size_in_bytes

    def _compute_byte_ranges(self):
        file_size_in_bytes = os.path.getsize(self.filename)
        self.total_size_in_bytes = file_size_in_bytes
        if self.custom_chunk_size:
            self.part_size = self.custom_chunk_size
            self._do_compute_byte_ranges(file_size_in_bytes, self.custom_chunk_size)
        else:
            if file_size_in_bytes > GiB:
                self.part_size = GiB
                self._do_compute_byte_ranges(file_size_in_bytes, GiB)
            else:
                self.part_size = MiB
                self._do_compute_byte_ranges(file_size_in_bytes, MiB)

    def _do_compute_byte_ranges(self, file_size_in_bytes, chunk_size_in_bytes):

        number_of_chunks = file_size_in_bytes / chunk_size_in_bytes
        final_chunk = file_size_in_bytes % chunk_size_in_bytes

        start_of_range = 0
        for _ in range(0, number_of_chunks):
            self.parts.append(ByteRange(start_of_range, 
                                        chunk_size_in_bytes,
                                        file_size_in_bytes))
            start_of_range += chunk_size_in_bytes

        if final_chunk:
            self.parts.append(ByteRange(start_of_range,
                                        final_chunk,
                                        file_size_in_bytes))

