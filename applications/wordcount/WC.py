import pmr
import os
from pmr import PilotMapReduce
import pdb

COORDINATION_URL="redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
NUMBER_OF_REDUCES=8

if __name__ == "__main__":
    pmr_spec=[]
    pmr_spec.append({ # Machine specific parameters
                    "service_url": 'fork://localhost',
                    "working_directory": os.getcwd()+'/agent',
                    "affinity_datacenter_label": 'eu-de-south-1',
                    "affinity_machine_label": 'mymachine-1',
                    "input_dir":os.getcwd()+'/input',
                    "temp_dir":os.getcwd()+'/temp',
                    "output_dir":os.getcwd()+'/output',
                    "pilot_data":os.getcwd()+'/temp/pilotdata',                    
                    "processes_per_node":8,

                    # chunk parameters
                    "chunker":os.getcwd()+'/wordcount_chunk.sh',
                    "chunk_type": 1,  # 1 for chunking based on files size , 2 for line wise.
                    "chunk_arguments":[ str( 32 * 1024*1024) ],
                    
                    # Map parameters                                                            
                    "map_walltime":100,
                    "map_number_of_processes":8,
                    "mapper":os.getcwd()+'/wordcount_map_partition.py',
                    "map_arguments":[],
                    "map_job_number_of_processes":"1",
                    
                    #reduce parameters
                    "reduce_walltime":100,
                    "reduce_number_of_processes":8,
                    "reducer":os.getcwd() + '/wordcount_reduce.py',
                    "reduce_arguments":[],
                    "reduce_job_number_of_processes":"1",
                    })
                    
    # Scale PMR to multiple machines just by adding multiple pmr specifications.
                    
    mr = PilotMapReduce.MapReduce(pmr_spec,NUMBER_OF_REDUCES,COORDINATION_URL)
    mr.MapReduceMain()
