import pmr
from pmr import PilotMapReduce

if __name__ == "__main__":
    pilots=[]
    """pilots.append({ "service_url": 'pbs-ssh://india.futuregrid.org',
                       "map_number_of_processes":8,
                       "reduce_number_of_processes":8,
                       "working_directory": "/N/u/pmantha/agent",
                       'processes_per_node':8,
                       "map_walltime":100,
                       "reduce_walltime":100,
                       "affinity_datacenter_label": 'eu-de-south', 
                       "affinity_machine_label": 'mymachine',
                       "input_dir":'/N/u/pmantha/data/',
                       "temp_dir":'/N/u/pmantha/temp',
                       "output_dir":'/N/u/pmantha/output',
                       "chunker":'/N/u/pmantha/PilotMapReduce/source/applications/wordcount/wordcount_chunk.sh',
                       "chunk_arguments":[ str(64 * 1024*1024) ],
                       "mapper":'/N/u/pmantha/PilotMapReduce/source/applications/wordcount/wordcount_map_partition.py',
                       "map_arguments":[],
                       "reducer":'/N/u/pmantha/PilotMapReduce/source/applications/wordcount/wordcount_reduce.py',
                       "reduce_arguments":[],
                       "pilot_data":'/N/u/pmantha/temp/pilotdata'
                      })"""
    pilots.append({ "service_url": 'pbs+ssh://sierra.futuregrid.org',
                       "map_number_of_processes":8,
                       "reduce_number_of_processes":8,
                       "working_directory": "/N/u/pmantha/agent",
                       "map_walltime":100,
                       "reduce_walltime":100,
                       "affinity_datacenter_label": 'eu-de-south-1', 
                       "affinity_machine_label": 'mymachine-1',
                       "input_dir":'/N/u/pmantha/data',
                       'processes_per_node':8,
                       "temp_dir":'/N/u/pmantha/temp',
                       "output_dir":'/N/u/pmantha/output',
                       "chunker":'/N/u/pmantha/PilotMapReduce/applications/wordcount/wordcount_chunk.sh',
                       "chunk_arguments":[ str( 32 * 1024*1024) ],
                       "mapper":'/N/u/pmantha/PilotMapReduce/applications/wordcount/wordcount_map_partition.py',
                       "map_arguments":[],
                       "reducer":'/N/u/pmantha/PilotMapReduce/applications/wordcount/wordcount_reduce.py',
                       "reduce_arguments":[],
                       "pilot_data":'/N/u/pmantha/temp/pilotdata',
                      })
    """ pilots.append({ "service_url": 'pbs-ssh://india.futuregrid.org',
                       "map_number_of_processes":8,
                       'processes_per_node':8,
                       "reduce_number_of_processes":8,
                       "working_directory": "/N/u/pmantha/agent",
                       "map_walltime":100,
                       "reduce_walltime":100,
                       "affinity_datacenter_label": 'eu-de-south-2', 
                       "affinity_machine_label": 'mymachine-2',
                       "input_dir":'/N/u/pmantha/data/128/',
                       "temp_dir":'/N/u/pmantha/temp',
                       "output_dir":'/N/u/pmantha/output',
                       "chunker":'/N/u/pmantha/PilotMapReduce/source/applications/wordcount/wordcount_chunk.sh',
                       "chunk_arguments":[ str(64 * 1024*1024) ],
                       "mapper":'/N/u/pmantha/PilotMapReduce/source/applications/wordcount/wordcount_map_partition.py',
                       "map_arguments":[],
                       "reducer":'/N/u/pmantha/PilotMapReduce/source/applications/wordcount/wordcount_reduce.py',
                       "reduce_arguments":[],
                       "pilot_data":'/N/u/pmantha/temp/'
                      })"""

                    
    chunk_type = 1 # chunk_type specifies bytes/line based chunking. 1= bytes based chunking
    mr = PilotMapReduce.MapReduce(pilots,8,"1","redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379",chunk_type)
    mr.MapReduceMain()
