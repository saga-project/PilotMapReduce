import sys
import os
import time
import pdb
import logging
import bliss.saga as saga
#FORMAT = "PMR - %(asctime)s - %(message)s"
#logging.basicConfig(format=FORMAT,level=logging.INFO,datefmt='%H:%M:%S')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('PMR')

import bliss.saga as saga
from pilot import PilotComputeService, ComputeDataService, PilotDataService, DataUnit, State
from mrfunctions import *

class MapReduce: 
    def __init__(self,pilots,nbr_reduces,coordination_url):
        self.pilots = pilots
        self.nbr_reduces = nbr_reduces
        self.coordination_url = coordination_url 
        self.map_number_of_processes=1
        self.reduce_number_of_processes=1
        self.chunk=""
        self.mapper=""
        self.reducer=""
        self.cmr={}
        self.chunk_type=1
        self.chunk_arguments=[]
        self.map_arguments=[]
        self.pilot_wd = {}
        self.reduce_arguments=[]  
        self.output=""              
        self.input_dus = []
        self.chunk_dus = []
        self.executables =[]
        self.reduce_du = {}
        self.output_du = []
        self.map_output_dus = []
        self.rdu_files = []
        self.allmapoutputfiles = {}
        self.cloudpilot = None
        self.finalpilot = None

    def pstart(self):
        logger.info(" Start pilot service ")
        self.compute_data_service=ComputeDataService()    
        self.pilot_compute_service=PilotComputeService(self.coordination_url)
        self.pilot_data_service=PilotDataService(self.coordination_url)
 
    def start_pilot_datas(self):
        logger.info(" Starting pilot datas of each pilot.... ")
        for pilot in self.pilots:
            logger.debug (" Create pilot_data at " + str(pilot['pd_service_url']) )
            pd_desc = { "service_url":pilot['pd_service_url'],
                        "size":100,
                        "affinity_datacenter_label": pilot['affinity_datacenter_label'],
                        "affinity_machine_label": pilot['affinity_machine_label']
                      }            
            ## cloud specific attributes
            if pilot.has_key('access_key_id'):
                pd_desc["access_key_id"] = pilot['access_key_id']
                self.cloudpilot = pilot
                self.finalpilot = pilot
            if pilot.has_key('secret_access_key'):
                pd_desc["secret_access_key"] = pilot['secret_access_key']
                
            logger.info ( " pilot description " + str(pd_desc) )            
            self.pilot_data_service.create_pilot( pilot_data_description=pd_desc )
        self.compute_data_service.add_pilot_data_service(self.pilot_data_service)
        logger.debug(" Pilot datas started .... ")

    def remote_files(self, input):
        input=saga.Url(input)         
        if input.scheme == '':        
            dir = saga.filesystem.Directory("sftp://localhost/"+str(input)) 
            return map(lambda i:input.get_path()+"/"+i, dir.list() ) 
        else:
            dir = saga.filesystem.Directory(input) 
            input.scheme = "ssh"            
            return map(lambda i:str(input)+"/"+i, dir.list() )             
      
    def load_input_data(self):
        logger.info(" Loading input data of each pilot.... ")
        for pilot in self.pilots:  
            if pilot.has_key('chunk_input_pd_url'):
                pd = DataUnit(du_url=pilot['chunk_input_pd_url'])
                self.chunk_dus.append(pd)           
            elif pilot.has_key('input'):
                data_unit_description = { "file_urls": self.remote_files(pilot['input']) ,
                                      "affinity_datacenter_label": pilot['affinity_datacenter_label'],
                                      "affinity_machine_label": pilot['affinity_machine_label']
                                    }
                # submit pilot data to a pilot store 
                self.input_dus.append(self.compute_data_service.submit_data_unit(data_unit_description))
        self.compute_data_service.wait()           
        
    def load_chunk_mapper_reducer(self):
        logger.info(" Registering chunk,mapper and reducer script of each pilot .... ") 
             
        chunk_unit_description = { "file_urls": [self.chunk],
                                   "affinity_datacenter_label": self.pilots[0]['affinity_datacenter_label'],
                                   "affinity_machine_label": self.pilots[0]['affinity_machine_label']}        
        if self.cloudpilot is not None:
            chunk_unit_description['affinity_datacenter_label'] = self.cloudpilot['affinity_datacenter_label']
            chunk_unit_description['affinity_machine_label'] = self.cloudpilot['affinity_machine_label']
                                   
        self.cmr['chunk'] = self.compute_data_service.submit_data_unit(chunk_unit_description) 
        
        mapper_unit_description = { "file_urls": [self.mapper],
                                   "affinity_datacenter_label": self.pilots[0]['affinity_datacenter_label'],
                                   "affinity_machine_label": self.pilots[0]['affinity_machine_label']} 
                                   
        if self.cloudpilot is not None:
            mapper_unit_description['affinity_datacenter_label'] = self.cloudpilot['affinity_datacenter_label']
            mapper_unit_description['affinity_machine_label'] = self.cloudpilot['affinity_machine_label']
                                                                                             
        self.cmr['mapper']=self.compute_data_service.submit_data_unit(mapper_unit_description) 
        
        reducer_unit_description = { "file_urls": [self.reducer],
                                   "affinity_datacenter_label": self.pilots[0]['affinity_datacenter_label'],
                                   "affinity_machine_label": self.pilots[0]['affinity_machine_label']} 

        if self.cloudpilot is not None:
            reducer_unit_description['affinity_datacenter_label'] = self.cloudpilot['affinity_datacenter_label']
            reducer_unit_description['affinity_machine_label'] = self.cloudpilot['affinity_machine_label']                                   
                                           
        self.cmr['reducer']=self.compute_data_service.submit_data_unit(reducer_unit_description) 
        
        # submit pilot data to a pilot store 
        self.compute_data_service.wait()        
        
    def start_pilot_jobs(self):
        logger.info(" Starting Pilot Jobs .... ")   
        for pilot in self.pilots:
            pilot_job_desc = {"service_url":pilot['pj_service_url'],                                                                                            
                              "affinity_datacenter_label": pilot['affinity_datacenter_label'],
                              "affinity_machine_label":pilot['affinity_machine_label'] }
            if pilot.has_key('number_of_processes'):
                pilot_job_desc['number_of_processes'] = pilot['number_of_processes']
            if pilot.has_key('walltime'):
                pilot_job_desc['walltime']=pilot['walltime']
            if pilot.has_key('working_directory'):
                pilot_job_desc['working_directory']=pilot['working_directory']
            if pilot.has_key('allocation'):
                pilot_job_desc['allocation']=pilot['allocation']
            if pilot.has_key('queue'):
                pilot_job_desc['queue']=pilot['queue']
            if pilot.has_key('access_key_id'):
                pilot_job_desc['access_key_id']=pilot['access_key_id']
                pilot_job_desc['secret_access_key']=pilot['secret_access_key']                  
                pilot_job_desc['vm_id']=pilot['vm_id']                              
                pilot_job_desc['vm_ssh_username']=pilot['vm_ssh_username']
                pilot_job_desc['vm_ssh_keyname']=pilot['vm_ssh_keyname']
                pilot_job_desc['vm_ssh_keyfile']=pilot['vm_ssh_keyfile']
                pilot_job_desc['vm_type']=pilot['vm_type']  
                
            if pilot.has_key('final') and self.cloudpilot is None:
                self.finalpilot = pilot                                                              
                                                              
            self.pilot_compute_service.create_pilot(pilot_compute_description=pilot_job_desc)
            logger.info( "Pilot on " + str(pilot['pj_service_url']) + " submitted .... ")
        self.compute_data_service.add_pilot_compute_service(self.pilot_compute_service)
    
    def group_paired(self,chunk_du):
        group_chunks={}
        for fname, info in chunk_du.list().items():
            seq=fname.split("-")[-1]
            if group_chunks.has_key(seq):
               group_chunks[seq] = group_chunks[seq] + info['pilot_data']
            else:
               group_chunks[seq] = info['pilot_data']
        return group_chunks.values()  
        
    def chunk_input_data(self):
        logger.info(" Chunk input data on each pilot.... ")   
        i=0
        
        for input_du in self.input_dus:             
            # create empty data unit for output data
            chunk_du_description = { "file_urls": [], 
                                     "affinity_datacenter_label": input_du.data_unit_description['affinity_datacenter_label'],
                                     "affinity_machine_label":input_du.data_unit_description['affinity_machine_label'] }  
                                     
            chunk_du = self.compute_data_service.submit_data_unit(chunk_du_description)
            self.compute_data_service.wait()
            
            # create compute unit
            logger.info('Chunked Input PD URL to reconnect - ' + str (chunk_du.get_url()) )
            i=0
            for input in input_du.list():
                compute_unit_description = {
                    "executable": "/bin/sh",
                    "arguments": [str((self.cmr['chunk'].list()).iterkeys().next()), input] + self.chunk_arguments,
                    "number_of_processes": 1,
                    "output": "chunk"+str(i)+".out",
                    "error": "chunk"+str(i)+".err",
                    "affinity_datacenter_label": input_du.data_unit_description['affinity_datacenter_label'],
                    "affinity_machine_label":input_du.data_unit_description['affinity_machine_label'] ,
                    # Put files stdout.txt and stderr.txt into output data unit
                    "output_data": [{ chunk_du.get_url(): ['*-chunk-*']} ],
                    "input_data":[self.cmr['chunk'].get_url(),input_du.get_url()] 
                    } 
                i = i + 1                                                                                          
                compute_unit = self.compute_data_service.submit_compute_unit(compute_unit_description)            
            self.chunk_dus.append(chunk_du)            
        self.compute_data_service.wait()        
       
        
    def submit_map_jobs(self):               
        logger.info(" Submit the Map jobs .... ")                               

        for chunk_du in self.chunk_dus:                                
            if self.chunk_type == 2:
                grouped_files = self.group_paired(chunk_du)
                for group_files in grouped_files:
                    chunk_du_description =  { "file_urls": group_files,
                                              "affinity_datacenter_label": chunk_du.data_unit_description['affinity_datacenter_label'],
                                              "affinity_machine_label":chunk_du.data_unit_description['affinity_machine_label'] }
                    cdu = self.compute_data_service.submit_data_unit(chunk_du_description)
                    cdu.wait()
                    map_output_desc = { "file_urls": [],
                                "affinity_datacenter_label": chunk_du.data_unit_description['affinity_datacenter_label'],
                                "affinity_machine_label":chunk_du.data_unit_description['affinity_machine_label'] }
                               
                    map_odu = self.compute_data_service.submit_data_unit(map_output_desc)                                              
                    self.map_output_dus.append(map_odu)
                    map_odu.wait()

                    l=cdu.list().keys()
                    l.sort()
                    map_job_description = {
                        "executable": "python " ,
                        "arguments": [str((self.cmr['mapper'].list()).iterkeys().next())] + l + [str(self.nbr_reduces)] + self.map_arguments,
                        "number_of_processes": self.map_number_of_processes,
                        "output": "map.out",                                              
                        "error": "map.err",
                        "affinity_datacenter_label": chunk_du.data_unit_description['affinity_datacenter_label'],                                            
                        "affinity_machine_label":chunk_du.data_unit_description['affinity_machine_label'],       
                        }                        
                    while cdu.get_url() == None:
                        cdu.wait()                    
                    map_job_description['input_data'] = [self.cmr['mapper'].get_url(), cdu.get_url()]                    

                    while map_odu.get_url() == None:                 
                        map_odu.wait()
                    map_job_description['output_data'] = [{map_odu.get_url():['*-sorted-map-part-*']}] 
                 
                    self.compute_data_service.submit_compute_unit(map_job_description)                             
            else:                                                                            
                for chunk, info in chunk_du.list().items():
                    if self.chunk_type == 1:
                        chunk_du_description =  { "file_urls": info['pilot_data'], 
                                                  "affinity_datacenter_label": chunk_du.data_unit_description['affinity_datacenter_label'],
                                                  "affinity_machine_label":chunk_du.data_unit_description['affinity_machine_label'] }
    
                                              
                    cdu = self.compute_data_service.submit_data_unit(chunk_du_description)
                    cdu.wait()
                    
                    map_output_desc = { "file_urls": [],
                                "affinity_datacenter_label": chunk_du.data_unit_description['affinity_datacenter_label'],
                                "affinity_machine_label":chunk_du.data_unit_description['affinity_machine_label'] }
                               
                    map_odu = self.compute_data_service.submit_data_unit(map_output_desc)                                              
                    self.map_output_dus.append(map_odu)
                    map_odu.wait()
                                        
                                  
                    map_job_description = {
                        "executable": "python " ,
                        "arguments": [str((self.cmr['mapper'].list()).iterkeys().next()), chunk,str(self.nbr_reduces)] + self.map_arguments,
                        "number_of_processes": self.map_number_of_processes,
                        "output": "map.out",                                              
                        "error": "map.err",
                        "affinity_datacenter_label": chunk_du.data_unit_description['affinity_datacenter_label'],                                            
                        "affinity_machine_label":chunk_du.data_unit_description['affinity_machine_label'],                                                                                             
                        }
                      
                    while cdu.get_url() == None:
                        logger.info("Chunk DU is Noneeeee ")
                        cdu.wait()                    
                    map_job_description['input_data'] = [self.cmr['mapper'].get_url(), cdu.get_url()]                    

                    while map_odu.get_url() == None:
                        logger.info("Map output DU  is Noneeeee ")                    
                        map_odu.wait()
                    map_job_description['output_data'] = [{map_odu.get_url():['*-sorted-map-part-*']}] 
                                                
                    self.compute_data_service.submit_compute_unit(map_job_description) 
        self.compute_data_service.wait()                        
        logger.info(" Map jobs Done.... ")                                                    


    def prepare_shuffles(self):
        for map_odu in self.map_output_dus:  
            for mapfile, info in map_odu.list().items(): 
                self.allmapoutputfiles[mapfile]=info['pilot_data'] 
           
        for i in range(int(self.nbr_reduces)):
            self.reduce_du[i]=[]
            reduce_files = filter(lambda k: k.split("-")[-1] == str(i) ,self.allmapoutputfiles.keys())             
            reduce_pds=[]                    
            for rf in reduce_files:
                reduce_pds = reduce_pds + self.allmapoutputfiles[rf]
            
            urls=[rp.split(':')[0] for rp in reduce_pds ]
            urls=list(set(urls))
            
            for url in urls:
                filtered_reduce_pds=[l for l in reduce_pds if l.startswith(url)]
                if url=='ssh' and self.cloudpilot is not None:
                    filtered_reduce_pds=[saga.Url(l).path for l in filtered_reduce_pds]          
                if self.finalpilot is None:
                    self.finalpilot = self.pilots[0]    
                reduce_desc = { "file_urls": filtered_reduce_pds,
                                "affinity_datacenter_label":self.finalpilot['affinity_datacenter_label'],
                                "affinity_machine_label":self.finalpilot['affinity_machine_label']        
                              }     
                   
                self.reduce_du[i]=self.reduce_du[i] + [self.compute_data_service.submit_data_unit(reduce_desc)]                           
        self.compute_data_service.wait() 
        
    def submit_reduce_jobs(self):
        logger.info(" Submitting Reduce Jobs .... ")  

        i = 0
        for reduce_du in self.reduce_du.values():            
            #print reduce_du.data_unit_description
            output_desc = { "file_urls": [],
                "affinity_datacenter_label": self.finalpilot['affinity_datacenter_label'],
                "affinity_machine_label": self.finalpilot['affinity_machine_label']
                } 

            self.output_du.append(self.compute_data_service.submit_data_unit(output_desc))
            self.compute_data_service.wait()           
            reduce_job_description = {
                    "executable":  "python ",
                    "arguments": [ str((self.cmr['reducer'].list()).iterkeys().next())] + [ ":".join(k.list().keys()) for k in reduce_du ] + self.reduce_arguments,
                    "number_of_processes": self.reduce_number_of_processes,
                    "input_data" : [self.cmr['reducer'].get_url()] + [k.get_url() for k in reduce_du ],
                    "output": "reduce.out",
                    "error": "reduce.err",
                    "affinity_datacenter_label": self.finalpilot['affinity_datacenter_label'],
                    "affinity_machine_label": self.finalpilot['affinity_machine_label'],
                    "output_data": [{self.output_du[i].get_url():['reduce-' + str(i) ]}]
                }   
            self.compute_data_service.submit_compute_unit(reduce_job_description)
            i = i + 1    
        self.compute_data_service.wait()
        logger.info(" Reduce jobs Done.... ")    

    def export_reduce_output(self):
        logger.info(" Exporting reduce data units to output directory .... ") 
        for output_du in self.output_du:
            output_du.export(self.output)
            
            
    def pstop(self):
        logger.info(" Terminate pilot Service ")
        self.compute_data_service.cancel()    
        self.pilot_compute_service.cancel()
        self.pilot_data_service.cancel()

    def MapReduceMain(self):
        st=time.time()                        
        self.pstart()
        self.start_pilot_datas()
        self.load_input_data()        
        self.load_chunk_mapper_reducer()        
        self.start_pilot_jobs()
        et = time.time()
        logger.info(" Setup time = " + str(round(et-st,2)) + "secs" )
        ct = time.time()
        self.chunk_input_data()          
        et = time.time()
        logger.info(" Chunk time = " + str(round(et-ct,2)) + "secs" ) 
        ct = time.time()                           
        self.submit_map_jobs()  
        et = time.time()
        logger.info(" Map time = " + str(round(et-ct,2)) + "secs" ) 
        ct = time.time()   
        self.prepare_shuffles()               
        et = time.time()
        logger.info(" shuffle time = " + str(round(et-ct,2)) + "secs" )     
        ct = time.time()      
        self.submit_reduce_jobs()    
        et = time.time()
        logger.info(" Reduce time = " + str(round(et-ct,2)) + "secs" )         
        self.export_reduce_output()
        self.pstop()
        et=time.time()    
        logger.info(" Total time to solution = " + str(round(et-st,2)) + "secs" )                             
        
        
        
