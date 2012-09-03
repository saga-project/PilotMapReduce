import pmr
import os
from pmr import PilotMapReduce
import pdb

COORDINATION_URL="redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
NUMBER_OF_REDUCES=8

if __name__ == "__main__":
    pmr_spec=[]

    pmr_spec.append({ # Machine specific parameters
                    "pj_service_url": 'ssh://india.futuregrid.org',
                    "working_directory": os.getcwd()+'/agent',
                    "affinity_datacenter_label": 'eu-de-south-1',
                    "affinity_machine_label": 'mymachine-1',
                    "input":"sftp://india.futuregrid.org/"+os.getcwd()+'/input',
                    "pd_service_url":"ssh://india.futuregrid.org/"+os.getcwd()+"/pilotdata",                    
                    "walltime":100,
                    "number_of_processes":8,
                    })

    pmr_spec.append({ # Machine specific parameters
                    "pj_service_url": 'ssh://hotel.futuregrid.org',
                    "working_directory": '/N/u/pmantha/agent',
                    "affinity_datacenter_label": 'eu-de-south-2',
                    "affinity_machine_label": 'mymachine-2',
                    "input":'sftp://hotel.futuregrid.org/N/u/pmantha/input',
                    "pd_service_url":"ssh://hotel.futuregrid.org/N/u/pmantha/agent/pilotdata",
                    "walltime":100,
                    "number_of_processes":8,
                    })
                    
    # Scale PMR to multiple machines just by adding multiple pmr specifications.
    mr = PilotMapReduce.MapReduce(pmr_spec,NUMBER_OF_REDUCES,COORDINATION_URL)
                    
                    
    mr.map_number_of_processes=1
    mr.reduce_number_of_processes=1
    mr.chunk="ssh://india.futuregrid.org/" + os.getcwd()+'/wc_chunk.sh'
    mr.mapper="ssh://india.futuregrid.org/" + os.getcwd()+'/wc_map_partition.py'
    mr.reducer="ssh://india.futuregrid.org/" + os.getcwd()+'/wc_reduce.py'
    mr.chunk_type=1
    mr.chunk_arguments=[ str(32*1024*1024) ]
    mr.map_arguments=[]
    mr.reduce_arguments=[]
    mr.output=os.getcwd()+'/output'
    mr.MapReduceMain()
