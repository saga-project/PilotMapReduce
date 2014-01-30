from pmr.Reducer import Reducer
import sys

if __name__ == "__main__":
    # Initialize Reduce job
    reduceJob = Reducer(sys.argv)         
    
    
    # reduce function
    for pName in reduceJob.partitionFiles:
        with open(pName) as infile:
            for line in infile:
                reduceJob.emit(line)
                
    ## Finalize reduce job   
    reduceJob.finalize()            