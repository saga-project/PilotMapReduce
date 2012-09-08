import pmr
import os
from pmr import PilotMapReduce
import pdb
import uuid

COORDINATION_URL="redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
NUMBER_OF_REDUCES=8

if __name__ == "__main__":
    pmr_spec=[]

    pmr_spec.append({
                  "pj_service_url":"ec2+ssh://aws.amazon.com/",
                  "pd_service_url":"s3://pilot-data-" + str(uuid.uuid1()),
                  "input":'/N/u/pmantha/gen_input/1GB',
                  "access_key_id": "AKIAJPGNDJRYIG5LIEUA",
                  "secret_access_key":"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",
                  "vm_id":"ami-e9aa1b80",
                  "vm_ssh_username":"ubuntu",
                  "vm_ssh_keyname":"radical",
                  "vm_ssh_keyfile":"radical.pem",
                  "vm_type":"c1.xlarge",
                  "number_of_processes": 1,
                  "vm_location":"",
                  "affinity_datacenter_label": 'us-east',
                  "affinity_machine_label": '',
                })

    # Scale PMR to multiple machines just by adding multiple pmr specifications.
    mr = PilotMapReduce.MapReduce(pmr_spec,NUMBER_OF_REDUCES,COORDINATION_URL)
                    
                    
    mr.map_number_of_processes=1
    mr.reduce_number_of_processes=1
    mr.chunk=  os.getcwd()+'/seqal_chunk.sh'
    mr.mapper= os.getcwd()+'/seqal_map_partition.py'
    mr.reducer= os.getcwd()+'/seqal_reduce.py'
    mr.chunk_type=2
    mr.chunk_arguments=[str(1902960)]
    mr.map_arguments=[]
    mr.reduce_arguments=[]
    mr.output=os.getcwd()+'/output'
    pdb.set_trace()
    mr.MapReduceMain()
