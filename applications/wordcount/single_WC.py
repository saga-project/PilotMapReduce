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
                    "input":"sftp://localhost/"+os.getcwd()+'/input',
                    # chunk_input_pd_url printed on screen on the first run of this example can be used from 2nd run 
                    # onwards to use the existing chunked data.      
                    # "chunk_input_pd_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:pds-b011fef0-f624-11e1-8412-e61f1322a75c:pd-b0120bde-f624-11e1-8412-e61f1322a75c:du-b73363e0-f624-11e1-8412-e61f1322a75c",
                    "pd_service_url":"ssh://localhost/"+os.getcwd()+"/pilotdata",                    
                    "walltime":100,
                    "number_of_processes":8,
                    })
                    
    # Scale PMR to multiple machines just by adding multiple pmr specifications.
    mr = PilotMapReduce.MapReduce(pmr_spec,NUMBER_OF_REDUCES,COORDINATION_URL)
                    
                    
    mr.map_number_of_processes=1
    mr.reduce_number_of_processes=1
    mr.chunk="ssh://localhost/" + os.getcwd()+'/wc_chunk.sh'
    mr.mapper="ssh://localhost/" + os.getcwd()+'/wc_map_partition.py'
    mr.reducer="ssh://localhost/" + os.getcwd()+'/wc_reduce.py'
    mr.chunk_type=1
    mr.chunk_arguments=[ str(32*1024*1024) ]
    mr.map_arguments=[]
    mr.reduce_arguments=[]
    mr.output=os.getcwd()+'/output'
    pdb.set_trace()
    mr.MapReduceMain()
