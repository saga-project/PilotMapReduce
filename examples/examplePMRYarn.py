import sys
import os
import time
import logging
import uuid
import traceback
from pilot import PilotComputeService, PilotDataService, ComputeDataService, State
from bigjob import logger 
import pmr

COORDINATION_URL = "redis://localhost:6379"

def wordCountJob():
    
    # List of Pilot-MapReduce descriptions    
    pmrDesc = []    
    pmrDesc.append({
                    'pilot_compute': { "service_url": 'fork://localhost',
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
                    'input_url'    : 'sftp://localhost/' + os.path.join(os.getcwd(), "../resources/data/wordcount")
                  })
    
    job = pmr.Yarn(pmrDesc, COORDINATION_URL) 
    job.setUpCluster()
    
    job.launchPilotOnYarn(8032)
    
    cus = []    
    for i in range(8):
        # start compute unit
        compute_unit_description = {
            "executable": "/bin/date",
            "arguments": [],
            "number_of_processes": 1,
            "output": "stdout.txt",
            "error": "stderr.txt"
        }            
        cus.append(job.submitJobtoPilotOnYarn(compute_unit_description))
    
    job.wait(cus)
    job.stopPilotOnYarn()
    job.stopCluster()
    
    
if __name__ == "__main__":
    wordCountJob()
