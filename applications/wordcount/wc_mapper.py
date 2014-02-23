import sys
from pmr.mapper import mapper
    
if __name__ == "__main__":
    # Initialize Map Job
    mapJob = myMapper(sys.argv)
          
    
    # map function    
    with open(mapJob.chunkFile) as fh:
        line = fh.read()
        for word in line.split():
            mapJob.emit(word, "%s,%s" % (word, 1))
                            
    # # Finalize map job  
    mapJob.finalize()
