import os
import pmr
from pmr import util


COORDINATION_URL = "redis://localhost:6379"

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
    
    # Create Hadoop Job
    job = pmr.Spark(pmrDesc, COORDINATION_URL)
    
    # setup Hadoop cluster
    job.setUpCluster()
    
    sparkContext = "spark://" + job.getSparkMaster() + ":7077" 
    
    # SAGA Job dictionary description of Spark WordCount example.
    
    """inputFilePath = os.getenv('HOME') + "/data"             
    sparkDesc = {  "executable": "pyspark $SPARK_HOME/python/examples/wordcount.py" ,
                    "arguments": [sparkContext, inputFilePath]                    
                 }"""

    # SAGA Job dictionary description of Spark pi example.
    sparkDesc = {  "executable": "pyspark $SPARK_HOME/python/examples/pi.py" ,
                    "arguments": [sparkContext]                    
                 }    
        
    # Submit Spark Job
    sparkJob = job.submitJob(sparkDesc)   
    
    # Wait for Spark Job 
    util.waitCUs(sparkJob) 
    
    # Tear down cluster    
    job.stopCluster()
    
    
if __name__ == "__main__":
    wordCountJob()
