import os
import pmr
from pmr import util
import time


COORDINATION_URL = "redis://localhost:6379"

ITERATIONS = 10
inputFiles = ["/Users/pmantha/out"]
clusters = [5]
nbrMappers = [1]



def wordCountJob():
    
    # List of Pilot-MapReduce descriptions    
    pmrDesc = []
    f = open("results.txt","w")
    f.write("Run;File;Start_Time;Number_Clusters;End_Time;Number_Cores\n")
 
    for m in  nbrMappers:   
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
            
            for r in range(3):                    
                for d in inputFiles:
                    for c in clusters:
                        # SAGA Job dictionary description of Spark Kmeans example.
                        st = time.time()
                        sparkDesc = {  "executable": "pyspark " + os.getcwd() + "/kmeansIter.py" ,
                                        "arguments": [sparkContext, d, c, m, ITERATIONS]                    
                                     }            
                        # Submit Spark Job
                        job.submitJob(sparkDesc)
                        et = time.time()
                        f.write("%s,%s,%s,%s,%s,%s\n" % (r,d,st,c,et,m))
                
        finally:           
            # Tear down cluster    
            job.stopCluster()
            f.close()
    
    
if __name__ == "__main__":
    wordCountJob()
