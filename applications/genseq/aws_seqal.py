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
                  "input":'/N/u/pmantha/gen_input/4GB',
                  "access_key_id": "AKIAJPGNDJRYIG5LIEUA",
                  #"chunk_input_pd_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:pds-816b20b6-fe00-11e1-b0c7-e61f1322a75c:pd-816b3218-fe00-11e1-b0c7-e61f1322a75c:du-9e4240a6-fe01-11e1-b0c7-e61f1322a75c",
                  #"chunk_input_pd_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:pds-762510ce-fad9-11e1-975f-e61f1322a75c:pd-762518c6-fad9-11e1-975f-e61f1322a75c:du-85ce672c-fada-11e1-975f-e61f1322a75c",
                  "chunk_input_pd_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:pds-73537afc-fe0d-11e1-8a19-e61f1322a75c:pd-735389e8-fe0d-11e1-8a19-e61f1322a75c:du-73fc4130-fe0f-11e1-8a19-e61f1322a75c",
                  "secret_access_key":"II1K6B1aA4I230tx5RALrd1vEp7IXuPkWu6K5fxF",
                  "vm_id":"ami-6973c200",
                  "vm_ssh_username":"ubuntu",
                  "vm_ssh_keyname":"radical",
                  "vm_ssh_keyfile":"radical.pem",
                  "vm_type":"m2.4xlarge",
                  "number_of_processes": 8,
                  "vm_location":"",
                  "affinity_datacenter_label": 'us-east',
                  "affinity_machine_label": '',
                })
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
    mr.MapReduceMain()
