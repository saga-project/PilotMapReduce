import pmr
import os
from pmr import PilotMapReduce
import pdb

COORDINATION_URL="redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
NUMBER_OF_REDUCES=8

if __name__ == "__main__":
    pmr_spec=[]

    pmr_spec.append({ # Machine specific parameters
                    "pj_service_url": 'fork://localhost',
                    "working_directory": os.getcwd()+'/agent',
                    "affinity_datacenter_label": 'eu-de-south-1',
                    "affinity_machine_label": 'mymachine-1',
                    "input":os.getcwd()+'/input',
                    "pd_service_url":"ssh://localhost/"+os.getcwd()+"/pilotdata",                    
                    "walltime":100,
                    "number_of_processes":8,
                    })
                    
    # Scale PMR to multiple machines just by adding multiple pmr specifications.
    mr = PilotMapReduce.MapReduce(pmr_spec,NUMBER_OF_REDUCES,COORDINATION_URL)
                    
                    
    mr.map_number_of_processes=1
    mr.reduce_number_of_processes=1
    mr.chunk=os.getcwd()+'/wordcount_chunk.sh'
    mr.mapper=os.getcwd()+'/wordcount_map_partition.py'
    mr.reducer=os.getcwd()+'/wordcount_reduce.py'
    mr.chunk_type=1
    mr.chunk_arguments=[ str(32*1024*1024) ]
    mr.map_arguments=[]
    mr.reduce_arguments=[]
    mr.output=os.getcwd()+'/output'
    mr.MapReduceMain()
