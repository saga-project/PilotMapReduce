import numpy as np
import logging
import sys
from pimr.clustering import kmeans

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Iterative')
        
def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        dist = np.sum((p - centers[i]) ** 2)
        if dist < closest:
            closest = dist
            bestIndex = i
    return bestIndex    

if __name__ == "__main__":
    # default parameters passed to map function 
    chunkFileNm = sys.argv[1]
    nbrReduces=int(sys.argv[2])
    sortedPartitionNbr=[]
    partitionNbr=[[] for _ in range(nbrReduces)]
    
    
    # map function    
    centersFile = open(sys.argv[3])
    pVectors = map(kmeans.parseVectorLine, open(chunkFileNm))
    temp = kmeans.sortPoints(centersFile)
    cVectors = map(kmeans.parseVector, temp)
    logger.info("Total number of datapoints/chunk is %s " % len(pVectors))
    for point in pVectors:
        bestIndex = closestPoint(point, cVectors)        
        l=hash(bestIndex)%int(nbrReduces)
        partitionNbr[l].append("%s\n" % ",".join([str(x) for x in point]))
        
    for i in range(0,nbrReduces):
        partitionName="part-"+str(i) 
        sortedPartitionNbr.append(open( chunkFileNm + "-sorted-map-"+ partitionName,'w'))

    for i in range(0,nbrReduces):
        partitionNbr[i].sort()
        for l in partitionNbr[i]:
            sortedPartitionNbr[i].write(l) 

    for i in range(0,nbrReduces): 
        sortedPartitionNbr[i].close()