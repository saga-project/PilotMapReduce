import os
from pimr.clustering import kmeans
COORDINATION_URL="redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
NUMBER_OF_CLUSTERS=8

if __name__ == "__main__":
    pimrSpec=[]

    pimrSpec.append({ # Machine specific parameters
                    "pj_service_url": 'pbs://sierra.futuregrid.org',
                    "working_directory": os.getcwd()+'/agent',
                    "affinity_datacenter_label": 'eu-de-south-1',
                    "affinity_machine_label": 'mymachine-1',
                    "input":"sftp://localhost/"+os.getcwd()+'/input',
                    "pd_service_url":"ssh://localhost/"+os.getcwd()+"/pilotdata",                    
                    "walltime":400,
                    "number_of_processes":64,
                    })
                    
    initCenter =   { "file_urls": [ os.path.join( os.getcwd(), "centers.txt") ],
                      "affinity_datacenter_label": 'eu-de-south-1',
                      "affinity_machine_label": 'mymachine-1'
                    }
                    
    convergeDist = 1        
    mapProcs = 1
    reduceProcs = 1    
    #nbrPoints = 1677721
    #nbrPoints = 5
                    
    # Scale PIMR to multiple machines just by adding multiple pimr specifications.        
    pimr = kmeans.kmeans(pimrSpec, COORDINATION_URL, NUMBER_OF_CLUSTERS, convergeDist, mapProcs, reduceProcs, nbrPoints, initCenter)
    pimr.run()
    
    
                    
        
