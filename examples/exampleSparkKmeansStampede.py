import os
import pmr
from pmr import util
import time


COORDINATION_URL = "redis://login3.stampede.tacc.utexas.edu:6379"

ITERATIONS = 10
inputFiles = ["/scratch/01539/pmantha/input/10000Points50Centers.csv","/scratch/01539/pmantha/input/1000oints500Centers.csv","/scratch/01539/pmantha/input/100Points5000Centers.csv" ]
clusters = [50,500,5000]
nbrMappers = [4,8,16]


def wordCountJob():
    
    # List of Pilot-MapReduce descriptions    
    f = open("results.txt","w+")
    f.write("Run;File;Start_Time;Number_Clusters;End_Time;Mappers\n")
 
    for m in  nbrMappers:   
        pmrDesc = []
        pmrDesc.append({
                    'pilot_compute': { "service_url": 'slurm+ssh://stampede.tacc.xsede.org',
                                       "working_directory": '/scratch/01539/pmantha/pilot-compute',
                                       "queue":"development",
                                       "project":"TG-MCB090174" ,
                                       "affinity_datacenter_label": 'eu-de-south-1',
                                       "affinity_machine_label": 'mymachine-1',
                                       "walltime":60,
                                       "number_of_processes": m*2,
                                     },
                    'pilot_data'   : { "service_url": "ssh://localhost/scratch/01539/pmantha/pilot-data",
                                       "size": 100,
                                       "affinity_datacenter_label": "eu-de-south",
                                       "affinity_machine_label": "mymachine-1"
                                     },
                  })
    
    
        try:        
            # Create Spark Job
            job = pmr.Spark(pmrDesc, COORDINATION_URL)
            
            # setup Spark cluster
            print "************************************************"
            print "starting a spark cluster with pilot desc: ", pmrDesc
            job.setUpCluster()
            print "************************************************"
            
            sparkContext = "spark://" + job.getSparkMaster() + ":7077" 
            
            for r in range(3):                    
                for ex in range(len(inputFiles)):
                    # SAGA Job dictionary description of Spark Kmeans example.
                    st = time.time()
                    sparkDesc = {   "executable": "SPARK_WORKER_DIR=$PWD;pyspark " + os.getcwd() + "/kmeansIter.py" ,
                                    "arguments": [sparkContext, inputFiles[ex], clusters[ex], m, ITERATIONS],
                                    "number_of_processes": 2
                                 }            
                    # Submit Spark Job
                    print "************************************************"
                    print "Submitting a Job with spark job description", sparkDesc
                    job.submitJob(sparkDesc)
                    print "************************************************"                    
                    et = time.time()
                    f.write("%s,%s,%s,%s,%s,%s\n" % (r,inputFiles[ex],st,clusters[ex],et,m))
                
        finally:           
            # Tear down cluster                
            job.stopCluster()
    f.close()

    
    
if __name__ == "__main__":
    wordCountJob()