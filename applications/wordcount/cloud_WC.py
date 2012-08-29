import pmr
import os
from pmr import PilotMapReduce
import pdb
import uuid

COORDINATION_URL="redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
NUMBER_OF_REDUCES=8

if __name__ == "__main__":
    pmr_spec=[]

    pmr_spec.append({ # Machine specific parameters
                    "pj_service_url": 'euca+ssh://149.165.146.135:8773/services/Eucalyptus',
                    "affinity_datacenter_label": 'us-east',
                    "affinity_machine_label": '',
                    "input":os.getcwd()+'/input',
                    "pd_service_url":"walrus://149.165.146.135/pilot-data-" + str(uuid.uuid1()),
                    "access_key_id":"KEEGSVWV0GTGGRDKIAJZ3",
                    "number_of_processes": 1,
                    "secret_access_key":"P0UZbVEtj8Qj1q2r0XQCcMVoBh7xoE7bCtlT8g4k",
                    "vm_id":"emi-49033B32",
                    "vm_ssh_username":"root",
                    "vm_ssh_keyname":"pradeep",
                    "vm_ssh_keyfile":"/N/u/pmantha/pradeep.pem",
                    "vm_type":"c1.xlarge",
                    })
                    
    # Scale PMR to multiple machines just by adding multiple pmr specifications.
    pdb.set_trace()
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
