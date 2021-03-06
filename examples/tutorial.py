import os
import pmr

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
                    'input_url'    : 'sftp://localhost/' + os.getenv("HOME") + "/data"
                  })
    
    job = pmr.MapReduce(pmrDesc, COORDINATION_URL)
    
    
    # SAGA Job dictionary description of Chunk, Map, Reduce tasks.         
    chunkDesc = { "executable": "split -l 50" }
    
    mapDesc   = { "executable": "python wc_mapper.py",
                  "number_of_processes": 1,
                  "spmd_variation":"single",
                  "files" : ['ssh://localhost/' + os.getenv("HOME") + "/wc_mapper.py"]
                }
    
    reduceDesc = { "executable": "python wc_reducer.py", 		  
                    "number_of_processes": 1,
                    "spmd_variation":"single",
                    "files" : ['ssh://localhost/' + os.getenv("HOME") + "/wc_reducer.py"]
                 }
    

    # Register Chunk, Map, Reduce tasks     
    job.setChunk(chunkDesc)
    job.setMapper(mapDesc)
    job.setReducer(reduceDesc)

    # Set number of reduces and output path.    
    job.setNbrReduces(8)
    job.setOutputPath(os.getenv("HOME") + "/output")
    
    # Submit Job.    
    job.runJob()
    
if __name__ == "__main__":
    wordCountJob()
