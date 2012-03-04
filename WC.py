import sys
import os
import time
import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot import PilotComputeService, PilotDataService, ComputeDataService, State
from mrfunctions import *


def has_finished(state):
    state = state.lower()
    if state=="done" or state=="failed" or state=="canceled":
        return True
    else:
        return False

def remote_files(base_dir, service_url):
    file_list=[]
    remote_url=saga.url(service_url)
    remote_url.scheme="ssh"
    jd = saga.job.description()
    jd.set_attribute("Executable", "/bin/ls")
    jd.set_attribute("Interactive", "True")
    jd.set_vector_attribute("Arguments", [base_dir])
    js = saga.job.service(remote_url);
    job = js.create_job(jd)
    job.run()
    output = job.get_stdout()
    for file in output:
        file_list.append(file.strip())
    remote_file_lists = map(lambda i:base_dir+"/"+i, file_list )
    output.close()
    del(jd)
    del(job)
    del(js)
    return remote_file_lists


class MapReduce:

    def __init__(self,pilots,nbr_reduces,pro_subjob,advert_host,chunk_type):

        self.log=open('mapreducelogfile','w')
        self.pilots = pilots
        self.nbr_reduces = nbr_reduces
        self.advert_host = advert_host
        self.pro_subjob = pro_subjob
        self.chunk_list = []
        self.sorted_part_file_list= []
        self.du_files = []
        self.div_reduces =[]
        self.reduce_files = []
        self.dus = []
        self.chunk_type = chunk_type
        self.mappilotjobs=[]
        self.reducepilotjobs=[]
        self.compute_data_service = ComputeDataService()
        self.pilot_compute_service = PilotComputeService()
        for pilot in self.pilots:
            if pilot.has_key('final'):
                self.final_pilot=pilot
    
    def move_final_files(self):
        export_url=saga.url(self.final_pilot['service_url'])
        export_url.scheme="ssh"
        jd = saga.job.description()
        jd.set_attribute("Executable", "/bin/cp")
        jd.set_attribute("Interactive", "True")
        jd.set_vector_attribute("Arguments", [self.final_pilot['temp_dir']+"/pilotdata/*/*", self.final_pilot['temp_dir'] ])
        js = saga.job.service(export_url);
        job = js.create_job(jd)
        job.run()
        error = job.get_stderr()
        job.wait(-1)
        error.close()

    def start_chunking(self):
        jobs=[]
        job_states={}
        result_map = {}
        for pilot in self.pilots:
            service_url = pilot['service_url']
            chunker = pilot['chunker']
            chunk_parameters = []
            chunk_parameters.append( pilot['input_dir'] )
            chunk_parameters.append( pilot['temp_dir'] )
            chunk_parameters = chunk_parameters + pilot['chunk_arguments']
            print service_url+ str(chunk_parameters)
            jd = saga.job.description()
            jd.executable = chunker
            jd.number_of_processes = "1"
            jd.arguments = chunk_parameters 
            jd.output=os.getcwd()+"/"+ "stroutput.txt"
            jd.error=os.getcwd()+"/"+"strerror.txt"
            chunk_url=saga.url(service_url)
            chunk_url.scheme="ssh"
            js = saga.job.service(chunk_url)
            job = js.create_job(jd)
            job.run()
            jobs.append(job)
            job_states[job]=str(job.get_state())

        # busy wait for completion
        while 1:            
            finish_counter=0
            for i in range(0, len(jobs)):
                old_state = job_states[jobs[i]]
                state = str(jobs[i].get_state())
                if result_map.has_key(state)==False:
                    result_map[state]=1
                else:
                    result_map[state] = result_map[state]+1
                if old_state != state:
                    print "Job " + str(jobs[i]) + " changed from: " + old_state + " to " + state
                if old_state != state and has_finished(state)==True:
                    print "Job: " + str(jobs[i]) 
                   
                if has_finished(state) == True:
                    finish_counter = finish_counter + 1

                job_states[jobs[i]]=state
            if finish_counter == len(jobs):
                break

        for pilot in self.pilots:   
            service_url = pilot['service_url']
            remote_file_list = remote_files( pilot['temp_dir'],str(service_url) )
            print remote_file_list
            mrf = mrfunctions(remote_file_list,self.chunk_type)
            self.chunk_list.append(mrf.group_chunk_files())
            print " Chunking completed...... \n"
            print self.chunk_list
            

    def start_map_pilot_jobs(self):
        for pilot in self.pilots:
            pilot_job_desc = {"service_url":pilot['service_url'], "number_of_processes": pilot['map_number_of_processes'],
                              "working_directory":pilot['working_directory'],"walltime":pilot['map_walltime'], 
                              "processes_per_node":pilot['processes_per_node'],
                              "affinity_datacenter_label": pilot['affinity_datacenter_label'],
                              "affinity_machine_label":pilot['affinity_machine_label'] }
            pilotjob = self.pilot_compute_service.create_pilot(pilot_compute_description=pilot_job_desc)
            self.mappilotjobs.append(pilotjob)
        self.compute_data_service.add_pilot_compute_service(self.pilot_compute_service)
    
    def start_reduce_pilot_jobs(self):
        pilot_job_desc = {"service_url":self.final_pilot['service_url'], "number_of_processes": self.final_pilot['reduce_number_of_processes'],
                          "working_directory":self.final_pilot['working_directory'],"walltime":self.final_pilot['reduce_walltime'], 
                          "affinity_datacenter_label": self.final_pilot['affinity_datacenter_label'],
                          "processes_per_node":self.final_pilot['processes_per_node'],
                          "affinity_machine_label":self.final_pilot['affinity_machine_label'] }
        pilotjob = self.pilot_compute_service.create_pilot(pilot_compute_description=pilot_job_desc)
        self.compute_data_service.add_pilot_compute_service(self.pilot_compute_service)

        
    def map_jobs(self):
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
                "stderr": "stderr"+ str(i) +"-" + str(j)+".txt",
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
        
        
        #Get the list of all map output files on all the machines.
        
        sorted_part_file_list = []
        for pilot in pilots:
            service_url = pilot['service_url']
            remote_file_list = remote_files( pilot['temp_dir'],str(service_url) )
            # filter sorted files.
            sorted_partion_files = filter( lambda x: 'sorted-part' in x, remote_file_list)
            # add ssh url to the map files. used by pilot data.
            map_url=saga.url(service_url)
            map_url.scheme="ssh"
            self.sorted_part_file_list.append( map(lambda i:str(map_url) +"/"+ i, sorted_partion_files ) )


        """for map_sorted_part_file_list in sorted_part_file_list:
            self.comb_map_sorted_part_file_list =  self.comb_map_sorted_part_file_list + map_sorted_part_file_list"""

    #Group map files related to a single reduce
    def group_map_files(self):
        for machine_files in self.sorted_part_file_list:
            for i in range(int(self.nbr_reduces)):
                self.du_files.append( filter(lambda k: k.split("-")[-1] == str(i) ,machine_files) )
 

    """#distributed reduces and its corresponding files to all machines equally.
    def split_pd(self):
        if self.nbr_reduces%len(self.pilots) != 0:
            dist = self.nbr_reduces/len(self.pilots) + 1
        else:
            dist = self.nbr_reduces/len(self.pilots)
        self.du_files = [ self.du_files[i:i+dist] for i in range(0, len(self.du_files), dist )]
        print "********ALL DU FILES********************************************************************"
        print self.du_files
        print "*********************************************************************************"""
        

    #start pilot datas on all the machines where intermediate data will be stored.
    def start_pilot_datas(self):
        service_url = self.final_pilot['service_url']
        map_url=saga.url(service_url)
        map_url.scheme="ssh"
        print " Create pilot_data at " + str(map_url) + self.final_pilot['temp_dir']
        pd_desc = { "service_url":str(map_url) + self.final_pilot['temp_dir']+"/pilotdata",
                    "size":100,
                    "affinity_datacenter_label": self.final_pilot['affinity_datacenter_label'],
                    "affinity_machine_label": self.final_pilot['affinity_machine_label']
                    }
        print pd_desc
        ps = self.pilot_data_service.create_pilot( pilot_data_description=pd_desc )
        self.compute_data_service.add_pilot_data_service(self.pilot_data_service)

    #submit data units to the pilot datas
    def create_pilot_descs(self):
        i=0
        for du in self.du_files:
            data_unit_description = { "file_urls":du,
                                    "affinity_datacenter_label": self.final_pilot['affinity_datacenter_label'],
                                    "affinity_machine_label": self.final_pilot['affinity_machine_label']
                                    }
            data_unit = self.compute_data_service.submit_data_unit(data_unit_description)
            print "************************DATA_UNIT_DESCRIPTION*******************************"
            print data_unit_description
            print "*****************************************************************************************"                                      
            self.dus.append(data_unit)
                    
        # Wait until the intermediate data transfer is completed.
        for duswait in self.dus:
            duswait.wait()            
            
    #export data units to the final temp directory
    def export_pilot_descs(self):
        export_url=saga.url(self.final_pilot['service_url'])
        export_url.scheme="ssh"
        for du in self.dus:
            du.export(str(export_url) + "/" + self.final_pilot['temp_dir'])


    #submit the reduce jobs.
    
    """def reduce_jobs(self):
        # start compute unit
        for du in self.du_files: 
            for reduce_du in machine_du:
                # get the paths. remove saga urls 
                k=map(lambda l:saga.url(l).path,reduce_du)
                # remove the absoule paths.
                k=map(lambda l:os.path.basename(l), k)
                
                compute_unit_description = {
                    "executable": self.pilots[pj]['reducer'],
                    "arguments": [":"+ ":".join(k), self.pilots[pj]['output_dir'] ] + self.pilots[pj]['reduce_arguments'],
                    "total_core_count": 1,
                    "number_of_processes": self.pro_subjob,
                    "working_directory": self.dus[dui].url,
                    "output": "stdredout-"+ str(dui)+".txt",
                    "stderr": "stdrederr-"+ str(dui)+".txt",
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
        etr=time.time()
        self.log.write("reduce jobs = " + str(round(etr-strt,2)) + "\n" )"""
        
    def reduce_jobs(self):
        remote_file_list=remote_files(self.final_pilot['temp_dir']+"/*sorted-part*", self.final_pilot['service_url'])
            
        for i in range(self.nbr_reduces):
            reduces=filter(lambda r: r.split("-")[-1] == str(i) ,remote_file_list) 
            # get the paths. remove saga urls 
            k=map(lambda l:saga.url(l).path,reduces)
            # remove the absoule paths.
            k=map(lambda l:os.path.basename(l), k)

            compute_unit_description = {
                 "executable": self.final_pilot['reducer'],
                 "arguments": [":"+ ":".join(k), self.final_pilot['output_dir'] ] + self.final_pilot['reduce_arguments'],
                 "total_core_count": 1,
                 "number_of_processes": self.pro_subjob,
                 "working_directory": self.final_pilot['temp_dir'],
                 "output": "stdredout-"+ str(i)+".txt",
                 "stderr": "stdrederr-"+ str(i)+".txt",
                 "affinity_datacenter_label": self.final_pilot['affinity_datacenter_label'],
                 "affinity_machine_label": self.final_pilot['affinity_machine_label']
                 }   
                 
            compute_unit = self.compute_data_service.submit_compute_unit(compute_unit_description)
            print "*************************** compute_unit_description ***********************************"
            print compute_unit_description
            print "*****************************************************************************************"   
            
        self.compute_data_service.wait()    

    def cancel(self):
        logging.debug("Terminate Pilot Data/Store Service")
        self.compute_data_service.cancel()    
        self.pilot_compute_service.cancel()
        self.pilot_data_service.cancel()
    
    def pstart(self):
        logging.debug("start Pilot Data/Store Service")
        self.compute_data_service=ComputeDataService()    
        self.pilot_compute_service=PilotComputeService()
        self.pilot_data_service=PilotDataService()

    def MapReduceMain(self):
        sync=900
        totst=time.time()
        st=time.time()
        ##  chunk
        self.start_chunking()
        et=time.time()
        self.log.write("chunk time = " + str(round(et-st,2)) + "\n" )  
        self.log.close()
        st=time.time()
        
        ## Start pilot jobs for map phase
        self.start_map_pilot_jobs()
        
        ## submit map jobs 
        self.map_jobs()
        et=time.time()
        self.log=open('mapreducelogfile','a')
        self.log.write("Map time = " + str(round(et-st,2)) + "\n" )  
        self.log.close()
        # cancel pilot jobs used for map phase
        self.compute_data_service.cancel()    
        self.pilot_compute_service.cancel()
        st=time.time()
        
        # start pilot compute,data services for intermediate data movement.
        self.pstart()
        
        # group map files relate to a reduce.
        self.group_map_files()
        
        # split data units between pilot datas
        # self.split_pd() 
        
        # create pilot datas
        self.start_pilot_datas()                
        
        # submit data units to pilot datas
        self.create_pilot_descs()
        time.sleep(sync)
        #self.export_pilot_descs()
        self.move_final_files()

        # log messages
        self.log=open('mapreducelogfile','a')
        et=time.time()
        self.log.write("Intermediate data transfer time = " + str(round(et-st,2)) + "\n" )
        self.log.close()

        # start pilot jobs for reduce phase        
        st=time.time()
        self.start_reduce_pilot_jobs()        

        # submit reduce jobs to pilot jobs
        self.reduce_jobs()
        et=time.time()
        self.log=open('mapreducelogfile','a')
        self.log.write("Reduce time = " + str(round(et-st,2)) + "\n" ) 
        self.log.close()

        self.cancel()
        totet=time.time()
        self.log=open('mapreducelogfile','a')
        self.log.write("Total time taken = " + str(round(totet - totst,2)) + "\n")
        self.log.close()


if __name__ == "__main__":
    pilots=[]
    """pilots.append({ "service_url": 'pbs-ssh://india.futuregrid.org',
                       "map_number_of_processes":32,
                       'processes_per_node':8,
                       "reduce_number_of_processes":32,
                       "working_directory": "/N/u/pmantha/agent",
                       "map_walltime":30,
                       "reduce_walltime":30,
                       "affinity_datacenter_label": 'india', 
                       "affinity_machine_label": 'india',
                       "input_dir":'/N/u/pmantha/wordcount_data/1GB',
                       "temp_dir":'/N/u/pmantha/temp',
                       "output_dir":'/N/u/pmantha/output',
                       "chunker":os.getcwd()+'/applications/wordcount/wordcount_chunk.sh',
                       "chunk_arguments":[ "2500000" ],
                       "mapper":os.getcwd()+'/applications/wordcount/wordcount_map_partition.py',
                       "map_arguments":["/N/u/pmantha/hg/hg19.fa"],
                       "reducer":os.getcwd()+'/applications/wordcount/wordcount_reduce.py',
                       "reduce_arguments":[],
                       "pilot_store":'/N/u/pmantha/temp/'
                      })"""

    pilots.append({ "service_url": 'pbs-ssh://hotel.futuregrid.org',
                       "map_number_of_processes":4,
                       'processes_per_node':8,
                       "reduce_number_of_processes":8,
                       "working_directory": "/N/u/pmantha/agent",
                       "map_walltime":30,
                       "reduce_walltime":30,
                       "affinity_datacenter_label": 'hotel',
                       "affinity_machine_label": 'hotel',
                       "input_dir":'/gpfs/scratch/pmantha/wc_data/sortdata/4GB/',
                       "temp_dir":'/gpfs/scratch/pmantha/temp',
                       "output_dir":'/gpfs/scratch/pmantha/output',
                       "chunker":os.getcwd()+'/applications/wordcount/wordcount_chunk.sh',
                       "chunk_arguments":[ str(128*1024*1024) ],
                       "mapper":os.getcwd()+'/applications/wordcount/wordcount_map_partition.py',
                       "map_arguments":[""],
                       "reducer":os.getcwd()+'/applications/wordcount/wordcount_reduce.py',
                       "reduce_arguments":[],
                       "final":'yes'
                      })

    pilots.append({ "service_url": 'pbs-ssh://sierra.futuregrid.org',
                       "map_number_of_processes":4,
                       'processes_per_node':8,
                       "reduce_number_of_processes":8,
                       "working_directory": "/N/scratch/pmantha/agent",
                       "map_walltime":100,
                       "reduce_walltime":100,
                       "affinity_datacenter_label": 'sierra',
                       "affinity_machine_label": 'sierra',
                       "input_dir":'/N/scratch/pmantha/wc_data/4GB',
                       "temp_dir":'/N/scratch/pmantha/temp',
                       "output_dir":'/N/scratch/pmantha/output',
                       "chunker":os.getcwd()+'/applications/wordcount/wordcount_chunk.sh',
                       "chunk_arguments":[ str(128*1024*1024) ],
                       "mapper":os.getcwd()+'/applications/wordcount/wordcount_map_partition.py',
                       "map_arguments":[""],
                      })
                      
    chunk_type =1 
    mr = MapReduce(pilots,8,"1","redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379",chunk_type)
    mr.MapReduceMain()

