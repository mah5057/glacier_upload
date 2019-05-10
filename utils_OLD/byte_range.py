class ByteRange():
    def __init__(self, start, chunk_size, total_size, uploaded=False):
        self.start = start
        self.chunk_size = chunk_size
        self.total_size = total_size
        self.uploaded = uploaded

    def was_uploaded(self):
        return self.uploaded
 
    def get_starting_byte(self):
        return self.start
    
    def get_final_byte(self):
        return self.start + self.chunk_size - 1

    def get_chunk_size(self):
        return self.chunk_size

    def get_range_string(self):
        return "bytes %s-%s/%s" % (self.start, self.start + self.chunk_size - 1, self.total_size)