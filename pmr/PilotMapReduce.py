import sys
import os
import time
import pdb
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('PMR')

import bliss.saga as saga
from pilot import PilotComputeService, ComputeDataService, PilotDataService, State
from mrfunctions import *


def remote_files(base_dir, url):
    k=saga.Url(url).host
    dir = saga.filesystem.Directory("sftp://"+k+"/"+base_dir)  
    return map(lambda i:base_dir+"/"+i, dir.list() ) 

class MapReduce:

    def __init__(self,pilots,nbr_reduces,pro_subjob,coordination_url,chunk_type):

        self.pilots = pilots
        self.nbr_reduces = nbr_reduces
        self.coordination_url = coordination_url
        self.pro_subjob = pro_subjob
        self.chunk_list = []
        self.comb_map_sorted_part_file_list = []
        self.du_files = []
        self.div_reduces =[]
        self.reduce_files = []
        self.dus = []
        self.chunk_type = chunk_type
        self.mappilotjobs=[]
        self.reducepilotjobs=[]
        self.coordinaton_url=""
        self.compute_data_service = ComputeDataService()
        self.pilot_compute_service = PilotComputeService(self.coordination_url)


    def start_chunking(self):
        chunkjobs=[]
        for pilot in self.pilots:
            service_url = pilot['service_url']            
            chunker = pilot['chunker']
            print chunker
            chunk_parameters = []
            chunk_parameters.append( pilot['input_dir'] )
            chunk_parameters.append( pilot['temp_dir'] )
            chunk_parameters = chunk_parameters + pilot['chunk_arguments']
            print chunk_parameters
            jd = saga.job.Description()
            jd.executable = chunker
            jd.arguments = chunk_parameters 
            jd.output = pilot['working_directory'] + "/chunk.log"
            jd.error = pilot['working_directory'] + "/chunk.err"
            chunk_url=saga.Url(service_url)
            chunk_url.scheme="ssh"
            js = saga.job.Service(chunk_url)         
            job =  js.create_job(jd)  
            job.run()
            chunkjobs.append(job)
              
        count = 0;
        for j in chunkjobs:
            if j.get_state() == saga.job.Job.Done:
                count = count + 1
            if count == len(self.pilots):
                break
                
        for pilot in self.pilots:
            service_url = pilot['service_url']
            chunk_url=saga.Url(service_url)
            chunk_url.scheme="ssh"            
            remote_file_list = remote_files(pilot['temp_dir'],str(chunk_url) )
            mrf = mrfunctions(remote_file_list,self.chunk_type)
            self.chunk_list.append(mrf.group_chunk_files())
            logger.info (" Chunking completed...... ")
            logger.info (self.chunk_list)
            

    def start_map_pilot_jobs(self):
        logger.info(" Starting Map Pilots....")
        for pilot in self.pilots:
            pilot_job_desc = {"service_url":pilot['service_url'], "number_of_processes": pilot['map_number_of_processes'],
                              "working_directory":pilot['working_directory'],"walltime":pilot['map_walltime'], 
                              "processes_per_node":pilot['processes_per_node'],
                              "affinity_datacenter_label": pilot['affinity_datacenter_label'],
                              "affinity_machine_label":pilot['affinity_machine_label'] }
            pilotjob = self.pilot_compute_service.create_pilot(pilot_compute_description=pilot_job_desc)
            self.mappilotjobs.append(pilotjob)
        self.compute_data_service.add_pilot_compute_service(self.pilot_compute_service)
        logger.info(" Map Pilots Started....")
        
    
    def start_reduce_pilot_jobs(self):
        logger.info(" Starting Reduce Pilots....")
        for pilot in self.pilots:
            pilot_job_desc = {"service_url":pilot['service_url'], "number_of_processes": pilot['reduce_number_of_processes'],
                              "working_directory":pilot['working_directory'],"walltime":pilot['reduce_walltime'], 
                              "affinity_datacenter_label": pilot['affinity_datacenter_label'],
                              "processes_per_node":pilot['processes_per_node'],
                              "affinity_machine_label":pilot['affinity_machine_label'] }
            pilotjob = self.pilot_compute_service.create_pilot(pilot_compute_description=pilot_job_desc)
            self.reducepilotjobs.append(pilotjob)
        self.compute_data_service.add_pilot_compute_service(self.pilot_compute_service)
        logger.info(" Reduce Pilots Started....")

        
    def map_jobs(self):
        logger.info(" Submitting Map Jobs....")
        jobs=[]
        job_start_times = {}
        job_states = {}
        i = 0
        
        for pilot in self.pilots:
            affinity_datacenter_label = pilot['affinity_datacenter_label']
            affinity_machine_label = pilot['affinity_machine_label'] 
            mapper = pilot['mapper']
            mapper_arguments = []
            mapper_arguments.append(str(self.nbr_reduces))
            mapper_arguments = mapper_arguments + pilot['map_arguments']
            temp_dir = pilot['temp_dir']
            j = 0
            for chunk_group in self.chunk_list[i]: 
                print " chunk " + "-".join(chunk_group) + " is being processed "
                # start work unit 
                compute_unit_description = {
                "executable": mapper,
                "arguments": chunk_group + mapper_arguments,
                "total_core_count": 1,
                "number_of_processes": self.pro_subjob,
                "working_directory": temp_dir,
                "output": "stdout"+ str(i) +"-" + str(j)+".txt",
                "error": "stderr"+ str(i) +"-" + str(j)+".txt",
                "affinity_datacenter_label": affinity_datacenter_label,              
                "affinity_machine_label": affinity_machine_label 
                }    
                work_unit = self.compute_data_service.submit_compute_unit(compute_unit_description)
                print " submitted jobs on resource -- " + str(i) + "-" + str(j)  
                j = j + 1
            i = i  + 1                                
        print "************************ All Map Jobs submitted ************************"
        print " No of map subjobs created - " + str(len(jobs))
        ############################################################################################
        # Wait for task completion of map tasks - synchronization    
        self.compute_data_service.wait()
        ############################################################################################
        logger.info(" Map Jobs Completed....")

        
        
        #Get the list of all map output files on all the machines.
        sorted_part_file_list = []
        for pilot in self.pilots:
            service_url = pilot['service_url']
            map_url=saga.Url(service_url)
            map_url.scheme="ssh"
            remote_file_list = remote_files( pilot['temp_dir'],str(map_url) )
            # filter sorted files.
            sorted_partion_files = filter( lambda x: 'sorted-part' in x, remote_file_list)
            # add ssh url to the map files. used by pilot data.
            sorted_part_file_list.append( map(lambda i:str(map_url) +"/"+ i, sorted_partion_files ) )


        for map_sorted_part_file_list in sorted_part_file_list:
            self.comb_map_sorted_part_file_list =  self.comb_map_sorted_part_file_list + map_sorted_part_file_list

    #Group map files related to a single reduce
    def group_map_files(self):
        logger.info(" Group all Map Output files by reduce .... ")
        for i in range(int(self.nbr_reduces)):
            self.du_files.append( filter(lambda k: k.split("-")[-1] == str(i) ,self.comb_map_sorted_part_file_list) )
        logger.info(" Grouping done .... ")
 

    #distributed reduces and its corresponding files to all machines equally.
    def split_pd(self):
        if self.nbr_reduces%len(self.pilots) != 0:
            dist = self.nbr_reduces/len(self.pilots) + 1
        else:
            dist = self.nbr_reduces/len(self.pilots)
        self.du_files = [ self.du_files[i:i+dist] for i in range(0, len(self.du_files), dist )]
        print "********ALL DU FILES********************************************************************"
        print self.du_files
        print "*********************************************************************************"
        

    #start pilot datas on all the machines where intermediate data will be stored.
    def start_pilot_datas(self):
        logger.info(" Starting Pilot Datas .... ")
        for pilot in self.pilots:
            service_url = pilot['service_url']
            map_url=saga.Url(service_url)
            map_url.scheme="ssh"
            print " Create pilot_data at " + str(map_url) + pilot['temp_dir']
            pd_desc = { "service_url":str(map_url) + pilot['temp_dir']+"/pilotdata",
                        "size":100,
                        "affinity_datacenter_label": pilot['affinity_datacenter_label'],
                        "affinity_machine_label": pilot['affinity_machine_label']
                       }
            print pd_desc
            ps = self.pilot_data_service.create_pilot( pilot_data_description=pd_desc )
        self.compute_data_service.add_pilot_data_service(self.pilot_data_service)
        logger.info(" Pilot Datas Started.... ")

    #submit data units to the pilot datas
    def create_pilot_descs(self):
        logger.info(" Transferring Intermediate Data via Pilot Data .... ")
        i=0
        for machine_du in self.du_files:
            for reduce_du in machine_du:
                print "---------------- " + str(reduce_du)
                data_unit_description = { "file_urls":reduce_du,
                                          "affinity_datacenter_label": self.pilots[i]['affinity_datacenter_label'],
                                          "affinity_machine_label": self.pilots[i]['affinity_machine_label']
                                        }
                data_unit = self.compute_data_service.submit_data_unit(data_unit_description)
                print "**********DDDDAATTAAA*****************- data_unit_description - " +str(i) + "***********************************"
                print data_unit_description
                print "*****************************************************************************************"  
                
                
                self.dus.append(data_unit)
            i=i+1
        # Wait until the intermediate data transfer is completed.
        for duswait in self.dus:
            duswait.wait()            
        logger.info(" Intermediate Data via PD transferred .... ")
            

    #submit the reduce jobs.
    
    def reduce_jobs(self):
        # start compute unit
        logger.info(" Submitting Reduce Jobs **************** " )
        jobs=[]
        job_start_times = {}
        job_states = {}
        pj = 0
        dui = 0
        for machine_du in self.du_files: 
            for reduce_du in machine_du:
                # get the paths. remove saga urls 
                k=map(lambda l:saga.Url(l).path,reduce_du)
                # remove the absoule paths.
                k=map(lambda l:os.path.basename(l), k)
                
                compute_unit_description = {
                    "executable": self.pilots[pj]['reducer'],
                    "arguments": [":"+ ":".join(k), self.pilots[pj]['output_dir'] ] + self.pilots[pj]['reduce_arguments'],
                    "number_of_processes": self.pro_subjob,
                    "input_data" : [self.dus[dui].get_url()],
                    "output": "stdredout-"+ str(dui)+".txt",
                    "error": "stdrederr-"+ str(dui)+".txt",
                    "affinity_datacenter_label": self.pilots[pj]['affinity_datacenter_label'],
                    "affinity_machine_label": self.pilots[pj]['affinity_machine_label']
                }   
                compute_unit = self.compute_data_service.submit_compute_unit(compute_unit_description)
                print "***************************- compute_unit_description - " +str(dui) + "***********************************"
                print compute_unit_description
                print "*****************************************************************************************"                
                dui=dui+1
            pj=pj+1
        
        self.compute_data_service.wait()    
        logger.info("Reduce Jobs Completed **************** " )
                
            
            
            

    def cancel(self):
        logger.info("Terminate Pilot Data/Store Service")
        self.compute_data_service.cancel()    
        self.pilot_compute_service.cancel()
        self.pilot_data_service.cancel()
    
    def pstart(self):
        logger.info("start Pilot Data/Store Service")
        self.compute_data_service=ComputeDataService()    
        self.pilot_compute_service=PilotComputeService(self.coordination_url)
        self.pilot_data_service=PilotDataService(self.coordination_url)
    


    def MapReduceMain(self):
        totst=time.time()
        st=time.time()
        ##  chunk
        self.start_chunking()
        et=time.time()
        logger.info("chunk time = " + str(round(et-st,2)) )  
        st=time.time()
        
        ## Start pilot jobs for map phase
        self.start_map_pilot_jobs()
        
        ## submit map jobs 
        self.map_jobs()
        et=time.time()
        
        logger.info("Map time =  "+ str(round(et-st,2)) ) 
        
        # cancel pilot jobs used for map phase
        self.compute_data_service.cancel()    
        self.pilot_compute_service.cancel()
        st=time.time()
        
        # start pilot compute,data services for intermediate data movement.
        self.pstart()
        
        # group map files relate to a reduce.
        self.group_map_files()
        
        # split data units between pilot datas
        self.split_pd() 
        
        # create pilot datas
        self.start_pilot_datas()                
        
        # submit data units to pilot datas
        self.create_pilot_descs()

        # log messages
        et=time.time()
        logger.info("Intermediate data transfer time = " + str(round(et-st,2)) )
        
        
        st=time.time()


        # start pilot jobs for reduce phase        
        self.start_reduce_pilot_jobs()        

        # submit reduce jobs to pilot jobs
        self.reduce_jobs()

        et=time.time()
       
        logger.info("Reduce time = " + str(round(et-st,2)) ) 
        

        self.cancel()
        totet=time.time()
        logger.info("Total time taken = " + str(round(totet - totst,2)) )
        
        


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
    mr = MapReduce(pilots,8,"1","redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379",chunk_type)
    mr.MapReduceMain()
