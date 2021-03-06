from pmr.reducer import Reducer
import sys
import numpy as np

CENTER_FILE_PREFIX = "Centers-"

def parseVector(line):
    return [float(x) for x in line.split(',')]


def average(points):
    numVectors = len(points)
    if numVectors > 0:
        pVectors = np.array(points)
        return list(pVectors.mean(0))
    else:
        return []
        
if __name__ == "__main__":
    # Initialize Reduce job
    reduceJob = Reducer(sys.argv)   
        
    # reduce function
    clusterPoints={}
    centersFile=open(CENTER_FILE_PREFIX + reduceJob.reduce, "w")
    for pName in reduceJob.partitionFiles:
        with open(pName) as infile:
            for line in infile:
                tokens=line.split(",")
                clusterId = tokens[0]
                if clusterPoints.get(clusterId,None) == None:
                    clusterPoints[clusterId] = []
                clusterPoints[clusterId].append(parseVector(",".join(tokens[1:])))
                reduceJob.emit( None,','.join(tokens[1:]) )                

    for clusterId,points in clusterPoints.iteritems():
        newCenter = average(points)
        centersFile.write("%s\n" % ",".join([str(x) for x in newCenter]))
    centersFile.close()

    ## Finalize reduce job   
    reduceJob.finalize() 