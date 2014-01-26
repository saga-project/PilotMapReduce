import sys
import os
import time
import pdb
import logging
import saga 
from pudb import set_interrupt_handler; set_interrupt_handler()
FORMAT = "%(asctime)s - %(message)s"
logging.basicConfig(format=FORMAT,level=logging.INFO,datefmt='%H:%M:%S')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('PMR')

from pilot import PilotComputeService, ComputeDataService, PilotDataService, DataUnit, State
from mrfunctions import *

class MapReduce:     
    def __init__(self, pilots, nbr_reduces, coordination_url ):
        self.pilots = pilots
        self.nbr_reduces = nbr_reduces
        self.coordination_url = coordination_url 
        self.map_number_of_processes=1
        self.reduce_number_of_processes=1
        self.chunk=""
        self.cdus=[]
        self.mapper=""
        self.reducer=""
        self.ms_name = ''
        self.rs_name = ''
        self.cs_name = ''
        self.iter_name = ''
        self.cs_url = None
        self.ms_url = None
        self.rs_url = None
        self.iter_url= None
        
        self.chunk_type=1
        self.chunk_arguments=[]
        self.chunk_cus =[]
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
        self.iterativeInput = None
        self.iterdu = None
        self.isIter = False
        
    def check_states(self, jobs):
        try:
            self.compute_data_service.wait()           
            jStates = map(lambda i: i.get_state(), jobs)
            logger.info(" No of CU/DU Units - %s, States - %s" % (str(len(jobs)), str(jStates)))
            self.print_job_details(jobs)
            """if 'Failed' in jStates:
                logging.info("Some of the tasks failed.. please check.. Terminating pilot service")
                self.pstop()
                sys.exit(0)"""
        except:
                self.pstop()
                sys.exit(0)
                
    def print_job_details(self,jobs):
        try:
            jDetails = map(lambda i: i.get_details(), jobs)
            for i in jDetails:
                logger.info("Task Details - %s" %  i)
        except:
            pass
                

        
    def pstart(self):
        """ Start Pilot compute and data Service """        
        logger.info(" Start pilot service ")
        self.compute_data_service=ComputeDataService()    
        self.pilot_compute_service=PilotComputeService(self.coordination_url)
        self.pilot_data_service=PilotDataService(self.coordination_url)        
 
    def start_pilot_datas(self):
        """ Create Pilot Datas  """    
    
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
        """ Create list of remote files used to create DU's """ 
        input=saga.Url(input)         
        if input.scheme == '':        
            dir = saga.filesystem.Directory("sftp://localhost/"+str(input)) 
            return map(lambda i:input.get_path()+"/"+i, dir.list() ) 
        else:
            dir = saga.filesystem.Directory(input) 
            input.scheme = "ssh"          
            return map(lambda i:str(input)+"/"+str(i), dir.list())             
      
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
        self.check_states(self.input_dus)

        
    def load_chunk_mapper_reducer(self):
        logger.info(" Registering chunk,mapper and reducer script .... ") 
             
        chunk_unit_description = { "file_urls": [self.chunk],
                                   "affinity_datacenter_label": self.pilots[0]['affinity_datacenter_label'],
                                   "affinity_machine_label": self.pilots[0]['affinity_machine_label']}        
        if self.cloudpilot is not None:
            chunk_unit_description['affinity_datacenter_label'] = self.cloudpilot['affinity_datacenter_label']
            chunk_unit_description['affinity_machine_label'] = self.cloudpilot['affinity_machine_label']
                                   
        cs_du = self.compute_data_service.submit_data_unit(chunk_unit_description) 
        self.cs_name = os.path.basename(self.chunk)
        
        mapper_unit_description = { "file_urls": [self.mapper],
                                   "affinity_datacenter_label": self.pilots[0]['affinity_datacenter_label'],
                                   "affinity_machine_label": self.pilots[0]['affinity_machine_label']} 
                                   
        if self.cloudpilot is not None:
            mapper_unit_description['affinity_datacenter_label'] = self.cloudpilot['affinity_datacenter_label']
            mapper_unit_description['affinity_machine_label'] = self.cloudpilot['affinity_machine_label']
                                                                                             
        ms_du=self.compute_data_service.submit_data_unit(mapper_unit_description) 
        self.ms_name = os.path.basename(self.mapper)

        
        reducer_unit_description = { "file_urls": [self.reducer],
                                   "affinity_datacenter_label": self.pilots[0]['affinity_datacenter_label'],
                                   "affinity_machine_label": self.pilots[0]['affinity_machine_label']} 

        if self.cloudpilot is not None:
            reducer_unit_description['affinity_datacenter_label'] = self.cloudpilot['affinity_datacenter_label']
            reducer_unit_description['affinity_machine_label'] = self.cloudpilot['affinity_machine_label']  
            
        rs_du=self.compute_data_service.submit_data_unit(reducer_unit_description) 
        self.rs_name = os.path.basename(self.reducer)

        
        # submit pilot data to a pilot store 
        # self.compute_data_service.wait()           
        self.check_states([cs_du,ms_du,rs_du])
        self.cs_url = cs_du.get_url()
        self.ms_url = ms_du.get_url()
        self.rs_url = rs_du.get_url()
        

        
    def load_iterative_data(self):
        ## Load iterative input data if present
        if self.iterativeInput:
            logger.info(" Loading iterative input .... ")         
            self.iter_name = os.path.basename(self.reducer)            
            self.iterdu = self.compute_data_service.submit_data_unit(self.iterativeInput)         
            # submit pilot data to a pilot store 
            # self.compute_data_service.wait() 
            self.check_states([self.iterdu])
            self.iter_url = self.iterdu.get_url()
        pass

    def get_iterative_du(self):        
        return self.iterdu
        
    def start_pilot_jobs(self):
        logger.info(" Starting Pilot Jobs .... ")               
        for pilot in self.pilots:
            pilot['service_url'] = pilot['pj_service_url']                                                              
            self.pilot_compute_service.create_pilot(pilot_compute_description=pilot)
            logger.info( "Pilot on " + str(pilot['service_url']) + " submitted .... ")
        self.compute_data_service.add_pilot_compute_service(self.pilot_compute_service)
    
    def group_paired(self,chunk_du):
        """ chunk method used for genome sequencing """
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
            chunk_du.wait()
            
            # create compute unit
            logger.info('Chunked Input PD URL to reconnect - ' + str (chunk_du.get_url()) )
            i=0
            idl = input_du.list()
            for input in idl:
                compute_unit_description = {
                    "executable": "/bin/sh",
                    "arguments": [str(self.cs_name), input] + self.chunk_arguments,
                    "number_of_processes": 1,
                    "output": "chunk"+str(i)+".out",
                    "error": "chunk"+str(i)+".err",
                    "affinity_datacenter_label": input_du.data_unit_description['affinity_datacenter_label'],
                    "affinity_machine_label":input_du.data_unit_description['affinity_machine_label'] ,
                    # Put files stdout.txt and stderr.txt into output data unit
                    "output_data": [{ chunk_du.get_url(): ['*-chunk-*']} ],
                    "input_data":[self.cs_url, input_du.get_url()] 
                    } 
                i = i + 1                                                                                          
                self.chunk_cus.append(self.compute_data_service.submit_compute_unit(compute_unit_description))
            self.chunk_dus.append(chunk_du)            
        self.check_states(self.chunk_cus + self.chunk_dus)
       
        
    def submit_map_jobs(self):               
        logger.info(" Submit the Map jobs .... ")                               
        k=0
        self.map_output_dus = []
        map_st = 0

        for chunk_du in self.chunk_dus:                                           
            if self.chunk_type == 2:
                """ Code only for genome sequencing """
                grouped_files = self.group_paired(chunk_du)
                for group_files in grouped_files:
                    chunk_du_description =  { "file_urls": group_files,
                                              "affinity_datacenter_label": chunk_du.data_unit_description['affinity_datacenter_label'],
                                              "affinity_machine_label":chunk_du.data_unit_description['affinity_machine_label'] }
                    cdu = self.compute_data_service.submit_data_unit(chunk_du_description)
                    #cdu.wait()
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

                    map_job_description['input_data'] = [self.cmr['mapper'].get_url(), cdu.get_url()]
                    if self.iterdu:
                        map_job_description['input_data'].append(self.iterdu.get_url())                    

                    map_job_description['output_data'] = [{map_odu.get_url():['*-sorted-map-part-*']}] 
                 
                    self.compute_data_service.submit_compute_unit(map_job_description)                             
            else:     
                map_data_st = time.time()
                iterChunk = chunk_du.list()
                if not self.isIter:                                                                       
                    for chunk, info in iterChunk.items():
                        dust = time.time()
                        chunk_du_description =  { "file_urls": info['pilot_data'], 
                                                  "affinity_datacenter_label": chunk_du.data_unit_description['affinity_datacenter_label'],
                                                  "affinity_machine_label":chunk_du.data_unit_description['affinity_machine_label'] }
                                                  
                        self.cdus.append(self.compute_data_service.submit_data_unit(chunk_du_description))
                        logger.info("chunk du created and took %s secs - " % str(round(time.time() - dust)))
                    
                
                for chunk, info in iterChunk.items():                    
                    dust = time.time()
                    map_output_desc = { "file_urls": [],
                                "affinity_datacenter_label": chunk_du.data_unit_description['affinity_datacenter_label'],
                                "affinity_machine_label":chunk_du.data_unit_description['affinity_machine_label'] }
                               
                    self.map_output_dus.append(self.compute_data_service.submit_data_unit(map_output_desc))  
                    logger.info("Map output du created and took %s secs - " % str(round(time.time() - dust)))
                                      
                self.check_states(self.cdus + self.map_output_dus)

                logger.info("DU preparation for Map tasks took %s secs" % str(round(time.time()-map_data_st,2)))

                map_jobs=[]
                map_st = time.time()
                mapper_script = str((self.cmr['mapper'].list()).iterkeys().next())
                mapper_url = self.cmr['mapper'].get_url()
                for cdu in self.cdus:
                    chunk_fn = str((cdu.list()).iterkeys().next())
                    logger.info("DU - %s - File name - %s  " % (str(cdu), chunk_fn ))
                    st = time.time()
                    logger.info("Creating a map compute unit..")                    
                    map_job_description = {
                        "executable": "python " ,
                        "arguments": [self.ms_name, chunk_fn ,str(self.nbr_reduces)] + self.map_arguments,
                        "number_of_processes": self.map_number_of_processes,
                        "output": "map.out",                                              
                        "error": "map.err",
                        "affinity_datacenter_label": cdu.data_unit_description['affinity_datacenter_label'],                                            
                        "affinity_machine_label": cdu.data_unit_description['affinity_machine_label'],                                                                                             
                        }                      
                    map_job_description['input_data'] = [self.ms_url, cdu.get_url()]    
                    if self.iterdu:
                        map_job_description['input_data'].append(self.iter_url)                                    
                    map_job_description['output_data'] = [{self.map_output_dus[k].get_url():['*-sorted-map-part-*']}]                      
                    map_jobs.append(self.compute_data_service.submit_compute_unit(map_job_description))
                    logger.info("Map compute unit submitted at-%s, and submission took %s secs" % (time.time(),str(round(time.time()-st,2))))
                    k=k+1

        logger.info("Total number of map jobs submitted " + str(k)) 
        self.check_states(map_jobs)
        logger.info("Only Map time took %s secs" % str(round(time.time()-map_st,2)))



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
        rvalues = self.reduce_du.values()
        for reduce_du in rvalues:            
            output_desc = { "file_urls": [],
                "affinity_datacenter_label": self.finalpilot['affinity_datacenter_label'],
                "affinity_machine_label": self.finalpilot['affinity_machine_label']
                } 
            
            self.output_du.append(self.compute_data_service.submit_data_unit(output_desc))            
        self.compute_data_service.wait()
        
        for reduce_du in rvalues:            
            reduce_job_description = {
                    "executable":  "python ",
                    "arguments": [ str(self.rs_name)] + [ ":".join(k.list().keys()) for k in reduce_du ] + self.reduce_arguments,
                    "number_of_processes": self.reduce_number_of_processes,
                    "input_data" : [self.rs_url] + [k.get_url() for k in reduce_du ],
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
    
            
    def get_reduce_output_dus(self):
        return self.output_du
            
    def clean_reduce_output(self):
        """logger.info(" Exporting reduce data units to output directory .... ") 
        for output_du in self.output_du:
            output_du.remove(self.output)"""
            
        pass
                                
    def pstop(self):
        logger.info(" Terminate pilot Service ")
        self.compute_data_service.cancel()    
        self.pilot_compute_service.cancel()
        self.pilot_data_service.cancel()

    def MapReduceMain(self):
        st=time.time() 
        if not self.isIter:
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
        self.load_iterative_data() 
        ct = time.time()
        self.submit_map_jobs()  
        et = time.time()
        #logger.info(" Map jobs submitted.... " + str(round(et-ct,2)) + "secs")                                                    
        et = time.time()
        #logger.info(" Map time = " + str(round(et-ct,2)) + "secs" ) 
        ct = time.time()   
        self.prepare_shuffles()  
        et = time.time()
        logger.info(" shuffle time = " + str(round(et-ct,2)) + "secs" )     
        ct = time.time()      
        self.submit_reduce_jobs()  
        et = time.time()
        logger.info(" Reduce time = " + str(round(et-ct,2)) + "secs" )         
        self.export_reduce_output()
        """self.pstop()
        et=time.time()    
        logger.info(" Total time to solution = " + str(round(et-st,2)) + "secs" )"""