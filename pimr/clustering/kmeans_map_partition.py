import time
import sys
from pmr.Mapper import Mapper
import logging
logger = logging.getLogger('MAPPER')
import pimr.clustering.kmeans as kmeans

def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        dist = sum([(m-k)**2 for k,m in zip(p,centers[i]) ])
        if dist < closest:
            closest = dist
            bestIndex = i
    return bestIndex

if __name__ == "__main__":
    # Initialize Map Job
    mapJob = Mapper(sys.argv)         
    
    # map function    
    pVectors = map(kmeans.parseVector, open(mapJob.chunkFileName))
    cVectors = map(kmeans.parseVector, open(mapJob.mapArgs[0]))
    cVectors = sorted(cVectors)    
    logger.info("Total number of datapoints/chunk is %s " % len(pVectors))
    tst=time.time()
    for point in pVectors:
        st=time.time()
        bestIndex = closestPoint(point, cVectors)                
        mapJob.emit(bestIndex, "%s,%s\n" % (bestIndex,",".join([str(x) for x in point])))        
        logger.info("Time taken - %s" % round(time.time()-st,2))
    logger.info("Total Time taken - %s" % round(time.time()-tst,2))
    
    ## Finalize map job  
    mapJob.finalize()