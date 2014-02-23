import os
from pmr import MapReduce


COORDINATION_URL="redis://localhost:6379"

def wordCountJob():
    chunkDesc = { "executable": "split -l 50" }
    
    mapDesc = { "executable": "python wc_mapper.py",
                  "number_of_processes": 1,
                  "spmd_variation":"single",
                  "files" : ['ssh://localhost/' + os.path.join(os.getcwd(), "../applications/wordcount/wc_mapper.py")]
                  }
    
    reduceDesc = { "executable": "python wc_reducer.py",    		  
    		   "number_of_processes": 1,
    		   "spmd_variation":"single",
               "files" : ['ssh://localhost/' + os.path.join(os.getcwd(), "../applications/wordcount/wc_reducer.py")]
    		 }
    
    pmrDesc = []
    
    pmrDesc.append({
                    'pilot_compute': { "service_url": "fork://localhost",
                                      "number_of_processes": 1,
                                      "working_directory": os.getenv("HOME")+"/pilot-compute",   
                                      "affinity_datacenter_label": "eu-de-south",              
                                      "affinity_machine_label": "mymachine-1"                                
                                      },
                    'pilot_data': {
                                   "service_url": "ssh://localhost/" + os.getenv("HOME")+"/pilot-data",
                                   "size": 100,   
                                   "affinity_datacenter_label": "eu-de-south",              
                                   "affinity_machine_label": "mymachine-1"                              
                                   },
                    'input_url': 'sftp://localhost/'+ os.path.join(os.getcwd(), "../resources/data/wordcount")
                    })
    

    job = MapReduce(pmrDesc, COORDINATION_URL)
    
    job.setNbrReduces(8)
    job.setOutputPath(os.getenv("HOME")+"/output")
    job.setChunk(chunkDesc)
    job.setMapper(mapDesc)
    job.setReducer(reduceDesc)
    
    job.runJob()
    
if __name__ == "__main__":
    wordCountJob()
    
    
