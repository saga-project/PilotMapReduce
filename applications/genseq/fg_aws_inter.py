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
                  "pj_service_url": 'fork://localhost',
                  "working_directory": os.getcwd()+'/agent',
                  "affinity_datacenter_label": 'eu-de-south-1',
                  "affinity_machine_label": 'mymachine-1',
                  "input":'sftp://localhost/N/u/pmantha/gen_input/test',
                  "pd_service_url":"ssh://localhost/"+os.getcwd()+"/pilotdata",
                  #"chunk_input_pd_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:pds-dbba20a2-fc85-11e1-a5a1-e61f1322a75c:pd-dbba347a-fc85-11e1-a5a1-e61f1322a75c:du-940df44e-fc86-11e1-a5a1-e61f1322a75c",
                  "chunk_input_pd_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:pds-0dfac728-fc96-11e1-bbab-e61f1322a75c:pd-0dfad3d0-fc96-11e1-bbab-e61f1322a75c:du-2efa56c2-fc97-11e1-bbab-e61f1322a75c",
                  #"chunk_input_pd_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:pds-97f2cfa2-fca5-11e1-9f0e-e61f1322a75c:pd-97f2dcfe-fca5-11e1-9f0e-e61f1322a75c:du-68dd2dbe-fca7-11e1-9f0e-e61f1322a75c",
                  "walltime":100,
                  "number_of_processes":8,
                })

    pmr_spec.append({
                  "pj_service_url":"ec2+ssh://aws.amazon.com/",
                  "pd_service_url":"s3://pilot-data-" + str(uuid.uuid1()),
                  "input":'/N/u/pmantha/gen_input/4GB',
                  "access_key_id": "AKIAJPGNDJRYIG5LIEUA",
                  "secret_access_key":"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",
                  #"chunk_input_pd_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:pds-dbba20a2-fc85-11e1-a5a1-e61f1322a75c:pd-dbf1b396-fc85-11e1-a5a1-e61f1322a75c:du-9467c708-fc86-11e1-a5a1-e61f1322a75c",
                  "chunk_input_pd_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:pds-0dfac728-fc96-11e1-bbab-e61f1322a75c:pd-0e338400-fc96-11e1-bbab-e61f1322a75c:du-2f05bdbe-fc97-11e1-bbab-e61f1322a75c",
                  #"chunk_input_pd_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:pds-97f2cfa2-fca5-11e1-9f0e-e61f1322a75c:pd-982c5812-fca5-11e1-9f0e-e61f1322a75c:du-69705e04-fca7-11e1-9f0e-e61f1322a75c",
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
