import sys
from pmr.mapper import Mapper
    
if __name__ == "__main__":
    # Initialize Map Job
    mapJob = Mapper(sys.argv)
          
    
    # map function    
    with open(mapJob.chunkFile) as fh:
        line = fh.read()
        for word in line.split():
            mapJob.emit(word, "%s,%s" % (word, 1))
                            
    # # Finalize map job  
    mapJob.finalize()
