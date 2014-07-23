import os
import pmr
from pmr import util


COORDINATION_URL = "redis://localhost:6379"

ITERATIONS = 10
inputFiles = ["/Users/pmantha/out"]
clusters = [5]
nbrMappers = 4



def wordCountJob():
    
    # List of Pilot-MapReduce descriptions    
    pmrDesc = []    
    pmrDesc.append({
                    'pilot_compute': { "service_url": "fork://localhost",
                                       "number_of_processes": 8,
                                       "working_directory": os.getenv("HOME") + "/pilot-compute",
                                       "affinity_datacenter_label": "eu-de-south",
                                       "affinity_machine_label": "mymachine-1"                                
                                     },
                    'pilot_data'   : { "service_url": "ssh://localhost/" + os.getenv("HOME") + "/pilot-data",
                                       "size": 100,
                                       "affinity_datacenter_label": "eu-de-south",
                                       "affinity_machine_label": "mymachine-1"                              
                                     },                    
                  })
    
    try:
        
        # Create Spark Job
        job = pmr.Spark(pmrDesc, COORDINATION_URL)
        
        # setup Spark cluster
        job.setUpCluster()
        
        sparkContext = "spark://" + job.getSparkMaster() + ":7077" 
        
        for dataFile in inputFiles:
            for nbrCluster in clusters:
                # SAGA Job dictionary description of Spark Kmeans example.
                sparkDesc = {  "executable": "pyspark " + os.getcwd() + "/kmeansIter.py" ,
                                "arguments": [sparkContext, dataFile, nbrCluster, nbrMappers, ITERATIONS]                    
                             }            
                # Submit Spark Job
                job.submitJob(sparkDesc)
            
    finally:           
        # Tear down cluster    
        job.stopCluster()
    
    
if __name__ == "__main__":
    wordCountJob()
