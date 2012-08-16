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

    def __init__(self,pilots,nbr_reduces,coordination_url):

        self.pilots = pilots
        self.nbr_reduces = nbr_reduces
        self.coordination_url = coordination_url
        self.chunk_list = []
        self.comb_map_sorted_part_file_list = []
        self.du_files = []
        self.div_reduces =[]
        self.reduce_files = []
        self.dus = []
        self.mappilotjobs=[]
        self.reducepilotjobs=[]
        self.coordinaton_url=""
        self.compute_data_service = ComputeDataService()
        self.pilot_compute_service = PilotComputeService(self.coordination_url)


    def start_chunking(self):
        logger.info(" Start chunking input data .... ")
        chunkjobs=[]
        for pilot in self.pilots:
            service_url = pilot['service_url']            
            chunk_url=saga.Url(service_url)
            try:
                saga.filesystem.Directory("sftp://"+ chunk_url.host + "/"+ pilot['input_dir'] )
            except:
                logger.info( " Input directory " + pilot['input_dir'] + " on " + chunk_url.host + " doesn't present" )
                self.pilots.remove(pilot)
                continue                
            
            t = saga.filesystem.Directory("sftp://"+ chunk_url.host + "/"+ pilot['temp_dir'], saga.filesystem.Create)
            if t:
                t.remove()
            saga.filesystem.Directory("sftp://"+ chunk_url.host + "/"+ pilot['temp_dir'], saga.filesystem.Create)            
            saga.filesystem.Directory("sftp://"+ chunk_url.host + "/"+ pilot['output_dir'], saga.filesystem.Create)

            chunk_url.scheme="ssh"
            chunker = pilot['chunker']
            chunk_parameters = []
            chunk_parameters.append( pilot['input_dir'] )            
            chunk_parameters.append( pilot['temp_dir'] )
            chunk_parameters = chunk_parameters + pilot['chunk_arguments']
            logger.debug (" chunk_parameters " + str(chunk_parameters) )
            
            jd = saga.job.Description()
            jd.executable = chunker
            jd.arguments = chunk_parameters 
            jd.output = pilot['working_directory'] + "/chunk.log"
            jd.error = pilot['working_directory'] + "/chunk.err"
            jd.number_of_processes = "1"
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
            chunk_type=pilot['chunk_type']
            chunk_url=saga.Url(service_url)
            chunk_url.scheme="ssh"            
            remote_file_list = remote_files(pilot['temp_dir'],str(chunk_url) )
            mrf = mrfunctions(remote_file_list,chunk_type)
            cl=mrf.group_chunk_files()
            self.chunk_list=[cl]
            logger.debug (" Chunking completed .... ")
            logger.info(" Chunked files on " + str(service_url) + " - " +str( self.chunk_list) )
            del mrf
            del cl
            return len(self.pilots)


    """def start_chunking(self):
        logger.info(" Start chunking input data .... ")
        chunkjobs=[]
        for pilot in self.pilots:
            service_url = pilot['service_url']            
            chunker = pilot['chunker']
            chunk_parameters = []
            chunk_parameters.append( pilot['input_dir'] )
            chunk_parameters.append( pilot['temp_dir'] )
            chunk_parameters = chunk_parameters + pilot['chunk_arguments']
            logger.debug (" chunk_parameters " + str(chunk_parameters) )
                                             
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
            logger.info("Remote file list")
            logger.info(remote_file_list)
            mrf = mrfunctions(remote_file_list,pilot['chunk_type'])
            logger.info(" grouped files ")            
            cl=mrf.group_chunk_files()
            self.chunk_list=[cl]
            logger.info(self.chunk_list)
            logger.debug (" Chunking completed .... ")
            logger.debug(" Chunked files on " + str(service_url) + " - " +str( self.chunk_list) )
            #logger.info(self.chunk_list)"""

    def start_map_pilot_jobs(self):
        logger.info(" Starting map pilots .... ")
        for pilot in self.pilots:
            pilot_job_desc = {"service_url":pilot['service_url'], "number_of_processes": pilot['map_number_of_processes'],
                              "working_directory":pilot['working_directory'],"walltime":pilot['map_walltime'], 
                              "processes_per_node":pilot['processes_per_node'],
                              "affinity_datacenter_label": pilot['affinity_datacenter_label'],
                              "affinity_machine_label":pilot['affinity_machine_label'] }
            pilotjob = self.pilot_compute_service.create_pilot(pilot_compute_description=pilot_job_desc)
            self.mappilotjobs.append(pilotjob)
            logger.info(" Map pilot on " + str(pilot['service_url']) + " started.... ")
        self.compute_data_service.add_pilot_compute_service(self.pilot_compute_service)
        logger.debug(" All map pilots started .... ")
        
    
    def start_reduce_pilot_jobs(self):
        logger.info(" Starting reduce pilots ....")
        for pilot in self.pilots:
            pilot_job_desc = {"service_url":pilot['service_url'], "number_of_processes": pilot['reduce_number_of_processes'],
                              "working_directory":pilot['working_directory'],"walltime":pilot['reduce_walltime'], 
                              "affinity_datacenter_label": pilot['affinity_datacenter_label'],
                              "processes_per_node":pilot['processes_per_node'],
                              "affinity_machine_label":pilot['affinity_machine_label'] }
            pilotjob = self.pilot_compute_service.create_pilot(pilot_compute_description=pilot_job_desc)
            self.reducepilotjobs.append(pilotjob)
            logger.info(" Reduce pilot on " + str(pilot['service_url']) + " started .... ")
        self.compute_data_service.add_pilot_compute_service(self.pilot_compute_service)
        logger.debug(" Reduce pilots started....")

        
    def map_jobs(self):
        logger.info(" Submitting map jobs .... ")
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
                #print " chunk " + "-".join(chunk_group) + " is being processed "
                # start work unit 
                compute_unit_description = {
                "executable": mapper,
                "arguments": chunk_group + mapper_arguments,
                "number_of_processes": pilot['map_job_number_of_processes'],
                "working_directory": temp_dir,
                "output": "stdout"+ str(i) +"-" + str(j)+".txt",
                "error": "stderr"+ str(i) +"-" + str(j)+".txt",
                "affinity_datacenter_label": affinity_datacenter_label,              
                "affinity_machine_label": affinity_machine_label 
                }    
                work_unit = self.compute_data_service.submit_compute_unit(compute_unit_description)
                #print " submitted jobs on resource -- " + str(i) + "-" + str(j)  
                j = j + 1
            i = i  + 1                                
        logger.debug(" All Map Jobs submitted ")
        logger.debug(" No of map subjobs created - " + str(len(jobs) ) )
        ############################################################################################
        # Wait for task completion of map tasks - synchronization    
        self.compute_data_service.wait()
        ############################################################################################
        logger.debug(" Map jobs completed ....")
                
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
        logger.info(" Group all map output files by reduce .... ")
        for i in range(int(self.nbr_reduces)):
            self.du_files.append( filter(lambda k: k.split("-")[-1] == str(i) ,self.comb_map_sorted_part_file_list) )
        logger.debug(" Grouping done .... ")
 

    #distributed reduces and its corresponding files to all machines equally.
    def split_pd(self):
        if self.nbr_reduces%len(self.pilots) != 0:
            dist = self.nbr_reduces/len(self.pilots) + 1
        else:
            dist = self.nbr_reduces/len(self.pilots)
        self.du_files = [ self.du_files[i:i+dist] for i in range(0, len(self.du_files), dist )]
        logger.debug(" DU Files " + str(self.du_files) )

    #start pilot datas on all the machines where intermediate data will be stored.
    def start_pilot_datas(self):
        logger.info(" Starting pilot datas .... ")
        for pilot in self.pilots:
            service_url = pilot['service_url']
            map_url=saga.Url(service_url)
            map_url.scheme="ssh"
            logger.debug (" Create pilot_data at " + str(map_url) + pilot['temp_dir'] )
            pd_desc = { "service_url":str(map_url) + pilot['temp_dir']+"/pilotdata",
                        "size":100,
                        "affinity_datacenter_label": pilot['affinity_datacenter_label'],
                        "affinity_machine_label": pilot['affinity_machine_label']
                       }
            logger.debug ( " pilot description " + str(pd_desc) )
            ps = self.pilot_data_service.create_pilot( pilot_data_description=pd_desc )
        self.compute_data_service.add_pilot_data_service(self.pilot_data_service)
        logger.debug(" Pilot datas started .... ")

    #submit data units to the pilot datas
    def create_pilot_descs(self):
        logger.info(" Transferring intermediate data via pilot data .... ")
        i=0
        for machine_du in self.du_files:
            for reduce_du in machine_du:
                data_unit_description = { "file_urls":reduce_du,
                                          "affinity_datacenter_label": self.pilots[i]['affinity_datacenter_label'],
                                          "affinity_machine_label": self.pilots[i]['affinity_machine_label']
                                        }
                data_unit = self.compute_data_service.submit_data_unit(data_unit_description)
                logger.debug(" Reduce data unit description " + str( data_unit_description) )
                                
                self.dus.append(data_unit)
            i=i+1
        # Wait until the intermediate data transfer is completed.
        self.compute_data_service.wait()         
        logger.debug(" Intermediate data via PD transferred .... ")
            

    #submit the reduce jobs.
    
    def reduce_jobs(self):
        # start compute unit
        logger.info(" Submitting reduce jobs .... " )
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
                    "number_of_processes": self.pilots[pj]['reduce_job_number_of_processes'],
                    "input_data" : [self.dus[dui].get_url()],
                    "output": "stdredout-"+ str(dui)+".txt",
                    "error": "stdrederr-"+ str(dui)+".txt",
                    "affinity_datacenter_label": self.pilots[pj]['affinity_datacenter_label'],
                    "affinity_machine_label": self.pilots[pj]['affinity_machine_label']
                }   
                compute_unit = self.compute_data_service.submit_compute_unit(compute_unit_description)
                logger.debug( " Reduce compute unit description " + str(compute_unit_description) )
                dui=dui+1
            pj=pj+1
        
        self.compute_data_service.wait()    
        logger.debug(" Reduce jobs completed .... " )
                
            
    def cancel(self):
        logger.info(" Terminate pilot Service ")
        self.compute_data_service.cancel()    
        self.pilot_compute_service.cancel()
        self.pilot_data_service.cancel()
    
    def pstart(self):
        logger.info(" Start pilot service ")
        self.compute_data_service=ComputeDataService()    
        self.pilot_compute_service=PilotComputeService(self.coordination_url)
        self.pilot_data_service=PilotDataService(self.coordination_url)
    


    def MapReduceMain(self):
        totst=time.time()
        st=time.time()
        ##  chunk
        np = self.start_chunking()
        ## if number of pilots > 0 then
        if np > 0 :
            et=time.time()
            logger.info(" Chunk time = " + str(round(et-st,2)) + "secs")  
            st=time.time()
        
            ## Start pilot jobs for map phase
            self.start_map_pilot_jobs()
            
            ## submit map jobs 
            self.map_jobs()
            et=time.time()
            
            logger.info(" Map time =  "+ str(round(et-st,2)) + "secs") 
            
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
            logger.info(" Intermediate data transfer time = " + str(round(et-st,2)) + "secs" )
            
            
            st=time.time()
    
    
            # start pilot jobs for reduce phase        
            self.start_reduce_pilot_jobs()        
    
            # submit reduce jobs to pilot jobs
            self.reduce_jobs()
    
            et=time.time()
           
            logger.info(" Reduce time = " + str(round(et-st,2)) + "secs" ) 
            
    
            self.cancel()
            totet=time.time()
            logger.info(" PilotMapReduce job completed and total time taken = " + str(round(totet - totst,2)) + "secs")
