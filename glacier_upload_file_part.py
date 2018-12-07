import hashlib
from botocore.utils import calculate_tree_hash

class GlacierUploadFilePart():

    def __init__(self, body, byte_range):
        self.body = body
        self.byte_range = byte_range

    def get_byte_range(self):
        return self.byte_range

    def get_body(self):
        return self.body

    def get_checksum(self):
        # from docs: Please note that this parameter is automatically populated if it is not provided. Including this parameter is not required
        # we can compute the treehash in the GlacierUploadFile using botocore.utils.calculate_tree_hash
        pass
