import os
from pimr.clustering import kmeans
COORDINATION_URL="redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
NUMBER_OF_CLUSTERS=5

if __name__ == "__main__":
    pimrSpec=[]
    pimrSpec.append({ # Machine specific parameters
                    "pj_service_url": 'slurm+ssh://stampede.tacc.xsede.org',
                    "working_directory": '/scratch/01539/pmantha/agent',
                    "queue":"development",
                    "project":"TG-MCB090174" , 
                    "affinity_datacenter_label": 'eu-de-south-1',
                    "affinity_machine_label": 'mymachine-1',
                    "input":"sftp://localhost/"+os.getcwd()+'/input',
                    "pd_service_url":"ssh://localhost/"+"/scratch/01539/pmantha/pilotdata",                    
                    "walltime":120,
                    "number_of_processes":128,
                    })

                    
    initCenter =   { "file_urls": [ os.path.join( os.getcwd(), "centers.txt") ],
                      "affinity_datacenter_label": 'eu-de-south-1',
                      "affinity_machine_label": 'mymachine-1'
                    }
                    
    convergeDist = 1        
    mapProcs = 2
    reduceProcs = 2    
    #nbrPoints = 1677721
    nbrPoints = 524288
    nbrIterations = 5
                    
    # Scale PIMR to multiple machines just by adding multiple pimr specifications.        
    pimr = kmeans.kmeans(pimrSpec, COORDINATION_URL, NUMBER_OF_CLUSTERS, convergeDist, mapProcs, reduceProcs, nbrPoints, initCenter, nbrIterations)
    pimr.run()