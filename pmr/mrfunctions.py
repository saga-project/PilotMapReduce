import bliss.saga as saga
import sys
import pdb
import os

class mrfunctions:
    
    def __init__(self,chunk_list,chunk_type):
        self.chunk_list = chunk_list
        self.chunk_type = chunk_type
        
    def group_chunk_files(self):
        if self.chunk_type == 1:
            return self.group_single()
        elif self.chunk_type == 2:
            return self.group_paired()

    def group_single(self):
        chunk_list = []
        for fname in self.chunk_list:
            chunk_list.append([fname])
        return chunk_list
        
    def group_paired(self):
        chunk_list = []
        group_chunks={}
        for fname in self.chunk_list:
            seq=fname.split("--")[1]
            if group_chunks.has_key(seq):
               group_chunks[seq].append(fname)
            else:
               group_chunks[seq] = fname.split()
        return group_chunks.values()   
