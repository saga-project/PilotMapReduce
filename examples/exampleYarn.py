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
    
    # Create Yarn Job
    job = pmr.Yarn(pmrDesc, COORDINATION_URL)
    
    # setup Yarn cluster
    job.setUpCluster()
    
    # SAGA Job dictionary description of Yarn Job.             
    hadoopDesc = {  "executable": "hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.2.0.jar wordcount",
                    "arguments": ['$HOME/data', '$HOME/Output']                    
                 }
    
    
    # Submit Yarn Job
    yarnJobs = []
    yarnJobs.append(job.submitJobtoYarn(hadoopDesc))
    
    job.wait(yarnJobs)
    
    # Tear down cluster    
    job.stopCluster()
    
    
if __name__ == "__main__":
    wordCountJob()
