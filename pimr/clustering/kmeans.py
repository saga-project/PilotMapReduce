import os
import time
import logging
import copy
import glob
from pmr import PilotMapReduce
import numpy as np
from pilot import PilotComputeService, PilotDataService, ComputeDataService, State
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Iterative')


def parseVector(line):
    return [float(x) for x in line.split(',')]

CENTER_FILE_PREFIX = "Centers-"

class kmeans:
    def __init__(self, pmrSpec, coordinationUrl, nbrReduces, delta, mapProcs, reduceProcs, nbrPoints, initCenter, nbrIterations = 10):
        self.pmrSpec = pmrSpec
        self.nbrReduces = nbrReduces
        self.delta = delta
        self.coordinationUrl = coordinationUrl
        self.mapProcs = mapProcs
        self.reduceProcs = reduceProcs
        self.centroid = initCenter
        self.tempDist = float('Inf')
        self.nbrPoints = nbrPoints
        self.nbrIterations=nbrIterations
        self.iterTimes = {}
        logger.info(" Initilalized Pilot-Iterative MapReduce ")
    
    def get_details(self):
        return self.iterTimes
        
    def run(self):            
        
        # Initialize PMR.
        mr = PilotMapReduce.MapReduce(self.pmrSpec, self.nbrReduces, self.coordinationUrl)
        mr.map_number_of_processes=self.mapProcs
        mr.reduce_number_of_processes=self.reduceProcs
        mr.chunk="ssh://localhost/" + os.getcwd()+'/../kmeans_chunk.sh'
        mr.mapper="ssh://localhost/" + os.getcwd()+'/../kmeans_map_partition.py'        
        mr.reducer="ssh://localhost/" + os.getcwd()+'/../kmeans_reduce.py'
        mr.chunk_type=1
        mr.chunk_arguments=[self.nbrPoints]
        initCenterFileName = os.path.basename(self.centroid['file_urls'][0])        
        mr.map_arguments=[initCenterFileName]  
        mr.reduce_arguments=[]
        mr.reduce_output_files = [CENTER_FILE_PREFIX+ '*']
        mr.output=os.getcwd()+'/output'
        logger.info("Initilalized Pilot-MapReduce ") 
        
        
        ofh = open(initCenterFileName) 
        oldVectors = map(parseVector, ofh)
        ofh.close()
        oldVectors = sorted(oldVectors)
        oldVectors = np.array(oldVectors)
        iterations = 1 
        iterDetails={}   
         
        while self.tempDist > self.delta and iterations <= self.nbrIterations:
            itst = time.time()
            mr.iterativeInput  = self.centroid            
            # Clean temporary iterative reduce output files 
            try:                
                for reduceOut in os.listdir(mr.output):
                    if reduceOut.startswith("reduce-"):
                        os.remove(os.path.join(mr.output,reduceOut))            
            except:
                pass
            
            mr.MapReduceMain()

            # Iteration execution statistics
            iterDetails = copy.copy(mr.get_details())
            
            # Merge centers
            mgst=time.time()                    
            newCenterFile = mr.output+'/centers.txt'
            with open(newCenterFile, 'w') as mergeFile:
                for centerOut in glob.glob( '%s/%s*' % (mr.output, CENTER_FILE_PREFIX)):
                    logger.info("processing file %s " % centerOut)
                    centerRead = open(os.path.join(mr.output,centerOut),'r')
                    for line in centerRead:
                        mergeFile.write(line)
                    centerRead.close()
            iterDetails['merge_centroids']=round(time.time()-mgst,2)   
                              
            self.centroid['file_urls'][0] = newCenterFile
            nfh = open(newCenterFile)
            newVectors = map(parseVector, nfh)            
            nfh.close()  
            newVectors = sorted(newVectors)   
            newVectors = np.array(newVectors)         
            
            self.tempDist = sum(np.sum((x - y) ** 2) for x,y in zip(oldVectors, newVectors)) 
            mr.isIter = True  
            oldVectors = newVectors
            itet = time.time()
            iterDetails['iteration_time'] = round(itet-itst,2)
            iterDetails['convergence'] = self.tempDist
            self.iterTimes[iterations] = iterDetails
            logger.info("Iteration Execution Times  - %s" % (self.iterTimes)) 
            iterations = iterations + 1                                  
        mr.pstop()