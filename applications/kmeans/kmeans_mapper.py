import time
import sys
from pmr.mapper import Mapper
import logging
import numpy as np
logger = logging.getLogger('MAPPER')

def parseVector(line):
    return [float(x) for x in line.split(',')]


def closestPoint(points, centers):
    bestIndex = 0
    closest = float("+inf")
    points = np.array(points)
    centers = np.array(centers)
    logger.debug("**closestPoint - Point: " + str(points) + " Centers: " + str(centers))
    for i in range(len(centers)):
        #dist = sum([(m-k)**2 for k,m in zip(points,centers[i]) ])
        dist = np.sum((points - centers[i]) ** 2)
        if dist < closest:
            closest = dist
            bestIndex = i
            logger.debug("Map point " + str(points) + " to index " + str(bestIndex))
    return bestIndex


if __name__ == "__main__":
    # Initialize Map Job
    mapJob = Mapper(sys.argv)         
    
    # map function    
    pVectors = map(parseVector, open(mapJob.chunkFile))
    cVectors = map(parseVector, open(mapJob.mapArgs[0]))
    #cVectors = sorted(cVectors)    
    print("Total number of datapoints/chunk is %s " % len(pVectors))
    tst=time.time()
    for point in pVectors:
        st=time.time()
        bestIndex = closestPoint(point, cVectors)                
        mapJob.emit(bestIndex, "%s,%s" % (bestIndex,",".join([str(x) for x in point])))        
        print("Time taken - %s" % round(time.time()-st,2))
    print("Total Time taken - %s" % round(time.time()-tst,2))
    
    ## Finalize map job  
    mapJob.finalize()