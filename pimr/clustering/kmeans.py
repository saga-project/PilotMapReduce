import os
import pdb
import numpy as np
import time
import logging


from pmr import PilotMapReduce
from pilot import PilotComputeService, PilotDataService, ComputeDataService, State
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Iterative')

def parseVector(point):
    return np.array(point)
    
def parseVectorLine(line):
    return np.array([float(x) for x in line.split(',')])  

def sortPoints(centersFile):
    temp = centersFile.readlines()
    temp = [ i.strip().split(",") for i in temp ]
    temp = [ ( float(i[0]), float(i[1]) ) for  i in temp ]    
    return sorted(temp)
    

def average(points):
    numVectors = len(points)
    if numVectors > 0:
        out = np.array(points[0])
        for i in range(1, numVectors):
            out += points[i]
        out = out / numVectors
        return out
    else:
        return np.array([0,0])

class kmeans:
    def __init__(self, pmrSpec, coordinationUrl, nbrClusters, delta, mapProcs, reduceProcs, nbrPoints, initCenter, nbrIterations = 10):
        self.pmrSpec = pmrSpec
        self.nbrClusters = nbrClusters
        self.delta = delta
        self.coordinationUrl = coordinationUrl
        self.mapProcs = mapProcs
        self.reduceProcs = reduceProcs
        self.centroid = initCenter
        self.tempDist = float('Inf')
        self.nbrPoints = nbrPoints
        self.nbrIterations=nbrIterations
        logger.info(" Initilalized Pilot-Iterative MapReduce ")

    def run(self):            
        # Scale PMR to multiple machines just by adding multiple pmr specifications.
        mr = PilotMapReduce.MapReduce(self.pmrSpec, self.nbrClusters, self.coordinationUrl)
        mr.map_number_of_processes=self.mapProcs
        mr.reduce_number_of_processes=self.reduceProcs
        mr.chunk="ssh://localhost/" + os.getcwd()+'/../kmeans_chunk.sh'
        mr.mapper="ssh://localhost/" + os.getcwd()+'/../kmeans_map_partition.py'        
        mr.reducer="ssh://localhost/" + os.getcwd()+'/../kmeans_reduce.py'
        mr.chunk_type=1
        mr.chunk_arguments=[self.nbrPoints]
        initCenterFileName = os.path.basename(self.centroid['file_urls'][0])
        mr.map_arguments=[initCenterFileName]  
        ofh = open(initCenterFileName) 
        temp = sortPoints(ofh)
        oldVectors = map(parseVector, temp)        
        mr.reduce_arguments=[]
        mr.output=os.getcwd()+'/output'
        logger.info(" Initilalized Pilot-MapReduce ") 
        iterations = 1     
        while self.tempDist > self.delta and iterations <= self.nbrIterations:
            logger.info("Iteration - %s, MaxIterations - %s " % (iterations , self.nbrIterations))
            itst = time.time()
            logger.info("Temp Distance - %s, Delta - %s" % (self.tempDist,self.delta) )
            mr.iterativeInput  = self.centroid   
            try:                
                for reduceOut in os.listdir(mr.output):
                    if reduceOut.startswith("reduce-"):
                         os.remove(os.path.join(mr.output,reduceOut))            
            except:
                pass
            
            st = time.time()    
            mr.MapReduceMain()
            et = time.time()
            
            logger.info("MapReduce is Done. Total time taken is " + str(round(et-st,2)) + "secs")

            newCenterFile = mr.output+'/centers.txt'
            with open(newCenterFile, 'w') as centerWrite:
                for reduceOut in os.listdir(mr.output):
                    newCentroid=np.array([0,0])
                    if reduceOut.startswith("reduce-"):
                        logger.info("processing file %s " % reduceOut)
                        outFile=open(os.path.join(mr.output,reduceOut),'r')
                        pVectors = map(parseVectorLine, outFile)
                        newCentroid = newCentroid + average(pVectors)
                        centerWrite.write("%s\n" % ",".join([str(x) for x in newCentroid]))
                        outFile.close()                        
            self.centroid['file_urls'][0] = newCenterFile
            nfh = open(newCenterFile)
            temp = sortPoints(nfh)
            newVectors = map(parseVector, temp)  
            logging.info("Old centers - %s, new centers - %s" % ( str(oldVectors),str(newVectors)))
            self.tempDist = sum(np.sum((x - y) ** 2) for x,y in zip(oldVectors, newVectors)) 
            mr.isIter = True  
            oldVectors = newVectors
            itet = time.time()
            logger.info("Iteration %s time - %s seconds" % (str(iterations), str( round(itet-itst,2) ) ))
            iterations = iterations + 1                      
        mr.pstop()
        
                    
                
                
            
            
            