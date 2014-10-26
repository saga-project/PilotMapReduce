import os, sys, time
import pmr
import logging
logger = logging.getLogger('DistributedInMemoryDataUnit-KMeans')
logger.setLevel(logging.DEBUG)
import numpy as np
import itertools
import datetime
import glob
import shutil


CENTER_FILE_PREFIX = "Centers-"

COORDINATION_URL = "redis://localhost:6379"
NUM_ITERATIONS = 2

class KMeans(object):

    @staticmethod
    def closestPoint(points, centers):
        bestIndex = 0
        closest = float("+inf")
        points = np.array([float(x) for x in points.split(",")])
        centers = np.array([[float(c.split(',')[0]), float(c.split(',')[1])] for c in centers])
        logger.debug("**closestPoint - Point: " + str(points) + " Centers: " + str(centers))
        for i in range(len(centers)):
            #dist = sum([(m-k)**2 for k,m in zip(points,centers[i]) ])
            dist = np.sum((points - centers[i]) ** 2)
            if dist < closest:
                closest = dist
                bestIndex = i
                logger.debug("Map point " + str(points) + " to index " + str(bestIndex))
        return (bestIndex, points.tolist())
    
    @staticmethod
    def averagePoints(points):
        logger.debug("Call average points on: " + str(points))
        points_extracted = [eval(i)[1] for i in points]
        points_np = np.array(points_extracted)
        new_center = np.mean(points_np, axis=0)
        logger.debug("New center: " + str(new_center))
        new_center_string = ','.join(['%.5f' % num for num in new_center])
        logger.debug("New center string: " + new_center_string) 
        return new_center_string 


def kmeans():    
    # List of Pilot-MapReduce descriptions    
    pmrDesc = []    
    pmrDesc.append({
                    'pilot_compute': { "service_url": "fork://localhost",
                                       "number_of_processes": 8,
                                       "working_directory": os.getenv("HOME") + "/pilot-compute",
                                       "affinity_datacenter_label": "eu-de-south",
                                       "affinity_machine_label": "mymachine-1"                                
                                     },
                    'pilot_data'   : { "service_url": "ssh://localhost" + os.getenv("HOME") + "/pilot-data",
                                       "size": 100,
                                       "affinity_datacenter_label": "eu-de-south",
                                       "affinity_machine_label": "mymachine-1"                              
                                     },
                    'input_url'    : 'sftp://localhost' + os.path.join(os.getcwd(), "../resources/data/kmeans")
                  })
        
    # SAGA Job dictionary description of Chunk, Map, Reduce tasks.         
    chunkDesc = { "executable": "split -l 100" }
    
    iterativeInput =   { "file_urls": [ os.path.join( os.getcwd(), "centers.txt") ],
                         "affinity_datacenter_label": 'eu-de-south-1',
                         "affinity_machine_label": 'mymachine-1'
                       }  
                           
    mapDesc   = { "executable": "python kmeans_mapper.py",
                  "number_of_processes": 1,
                  "spmd_variation":"single",
                  "files" : ['ssh://localhost' + os.path.join(os.getcwd(), "../applications/kmeans/kmeans_mapper.py")]
                }
    
    reduceDesc = { "executable": "python kmeans_reducer.py",           
                    "number_of_processes": 1,
                    "spmd_variation":"single",
                    "files" : ['ssh://localhost' + os.path.join(os.getcwd(), "../applications/kmeans/kmeans_reducer.py")]
                 }
    


    job = pmr.MapReduce(pmrDesc, COORDINATION_URL)

    # Register Chunk, Map, Reduce tasks     
    job.setChunk(chunkDesc)
    job.setMapper(mapDesc)
    job.setReducer(reduceDesc)

    # Set number of reduces and output path.    
    job.setNbrReduces(8)
    outputPath=os.getenv("HOME") + "/output"  
    shutil.rmtree(outputPath, ignore_errors=True)     
  
    job.setOutputPath(outputPath)        
    job.startPilot()
    job.initialize()
    job.setIterativeOutputPrefix([CENTER_FILE_PREFIX])
        
    for iteration in range(0,NUM_ITERATIONS):          
        job.setIterativeDataUnit(job.submitDataUnit(iterativeInput))
        job.mapReduce()        
        # Merge centers
        newCenterFile = os.path.join(job.outputPath, 'centers.txt')
        with open(newCenterFile, 'w') as mergeFile:
            for centerOut in glob.glob( '%s/%s*' % (job.outputPath, CENTER_FILE_PREFIX)):
                logger.info("processing file %s " % centerOut)
                centerRead = open(os.path.join(job.outputPath,centerOut),'r')
                for line in centerRead:
                    mergeFile.write(line)
                centerRead.close()
        iterativeInput["file_urls"]=[newCenterFile] 
    job.stopPilot()        
    
if __name__ == "__main__":
    kmeans()
