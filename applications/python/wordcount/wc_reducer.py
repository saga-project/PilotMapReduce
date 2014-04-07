from pmr.reducer import Reducer
import sys


if __name__ == "__main__":
    # Initialize Reduce job
    reduceJob = Reducer(sys.argv)         
    
    # Reduce function    
    count = {}        
    # split the map emitted to get words count from each partition file                       
    for pName in reduceJob.partitionFiles:
        with open(pName) as infile:
            for line in infile:
                tokens = line.split(",")
                
                # Actual word might contain "," and count is last token. 
                value = int(tokens[-1])
                word = ",".join(tokens[:-1])
                
                if count.has_key(word):
                    count[word] = count[word] + value
                else:
                    count[word] = value

    for word, count in count.iteritems():                
        reduceJob.emit(word, count)                

    # Finalize reduce job   
    reduceJob.finalize() 
