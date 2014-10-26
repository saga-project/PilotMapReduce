#!/usr/bin/env python

__author__ = "Pradeep Mantha"
__copyright__ = "Copyright 2011, Pradeep Mantha"
__license__ = "MIT"


from pilot import PilotComputeService, ComputeDataService, PilotDataService, DataUnit
from pmr import util
from pmr.util import constant
from pmr.util.logger import logger
from urlparse import urlparse
import copy, os, shutil


class MapReduce(object):
    
    """
        MapReduce: Class for managing MapReduce Jobs
    
    """ 
      
     
    def __init__(self, pmrDesc, coordinationUrl):
        """ 
            Initializes MapReduce with Pilot computes/Data description and
            coordination system 
            
        """
            
        logger.debug("Initialize Pilot-MapReduce")
        
        # Class variables.
        self._pilots = pmrDesc
        self._coordinationUrl = coordinationUrl        
        self._pilotComputes = []
        
        self._inputDus = []
        self._pdFTP = "ssh"
        
        self._chunkDus = []
        self._chunkDesc = None
        self._chunkExe = None
        
        self._mapDus = []
        self._mapDesc = None
        self._mapExe = None
        
        self.reduceDus = []
        self._reduceDesc = None
        self._reduceExe = None
        
        self._nbrReduces = 1        
        self._outputDu = None

        self._iterDu =  None
        self._iterOutputPrefixes = None

        self._pilotInfo = [{}] * len(self._pilots)
        
        self.pdUrl = urlparse(self._pilots[0]['pilot_data']["service_url"])
                    
        
        
        
        self.compute_data_service = None
        self.pilot_compute_service = None
        self.pilot_data_service = None

    def startPilot(self):
        """ Start the pilot compute and data services """
        
        logger.debug("Start pilot service")
        try:
            self.compute_data_service = ComputeDataService()    
            self.pilot_compute_service = PilotComputeService(self._coordinationUrl)
            self.pilot_data_service = PilotDataService(self._coordinationUrl)  
            self._startPilotComputeDatas()
        except Exception, ex:
            self._clean(ex, "Pilot service initialization failed - abort")
    
    
        
    def stopPilot(self):
        """ Stops the pilot compute and data services """
        logger.debug("Terminate pilot Service")
        try:
            self.compute_data_service.cancel()    
            self.pilot_compute_service.cancel()
            self.pilot_data_service.cancel()
        except Exception, ex:
            raise Exception ("Pilot service termination failed - abort")          
        
    def setNbrReduces(self, nbrReduces):
        """ 
            Set the number of reduces of the MapReduce Job 
            @param  nbrReduces: Takes number of Reduces as integer
                         
        """        
        self._nbrReduces = nbrReduces
        
    def setChunk(self, chunkDesc):
        """ 
            Registers the chunk task description
            @param  chunkDesc: SAGA Job Description of chunk task
                         
        """
        
        self._chunkDesc = chunkDesc
    
                
    def setMapper(self, mapDesc):
        """ 
            Registers the Map task description
            @param  mapDesc: SAGA Job Description of Map task
                         
        """
        
        self._mapDesc = mapDesc
        
    def setReducer(self, reduceDesc):
        """ 
            Registers the Reduce task description
            @param  reduceDesc: SAGA Job Description of Reduce task
                         
        """        
        self._reduceDesc = reduceDesc

    def setOutputPath(self, path):
        """ 
            Sets the output path to store the final results of MapReduce job
            @param  reduceDesc: SAGA Job Description of Reduce task
                         
        """          
        self.outputPath = path
        
        
    def _clean(self, ex, msg):
        """ Stops  the pilot compute and data services """

        print "Error: %s" % ex
        self.stopPilot()
        print msg
                
    def _startPilotComputeDatas(self):
        """ Starts  the pilot compute and data services """
        
        def create(pilot):
            self._pilotComputes.append(self.pilot_compute_service.create_pilot(pilot['pilot_compute']))
            self.pilot_data_service.create_pilot(pilot['pilot_data'])

        map(create, self._pilots)
            
        self.compute_data_service.add_pilot_compute_service(self.pilot_compute_service)
        self.compute_data_service.add_pilot_data_service(self.pilot_data_service)
        util.waitPilots(self._pilotComputes)  
        
        
    def getPilotComputes(self):
        """ Get pilot computes """
        return self._pilotComputes
      
                  
        
    def _loadDataIntoPD(self):
        """ Loads input data and executables into Pilot-Data """

        logger.debug("Loading input data into Pilot-Data")
        try:
            self._loadInputData()
            self._loadExecutables()
        except Exception, ex:
            self._clean(ex, "Loading input data failed - abort")
            


    def _loadInputData(self):
        """ Loads  input data into Pilot-Data """
        
        for pilot in self._pilots:
            if pilot['input_url'].startswith('redis'):
                # reconnect to Pilot-Data
                self._inputDus.append(util.getDuUrl(pilot['input_url']))                
            else:      
                desc = util.getEmptyDU(pilot['pilot_compute'])
                desc['file_urls'] = util.getFileUrls(pilot['input_url'], self._pdFTP)
                temp = self.compute_data_service.submit_data_unit(desc)
                pilot['input_url'] = temp.get_url()
                self._inputDus.append(temp)
        util.waitDUs(self._inputDus)
        
        logger.debug("New Pilot-MapReduce descriptions with updated PD URLS \n"  \
                    "use these descriptions to reuse already uploaded data")
        map(lambda x: logger.debug(x), self._pilots)
        
    def _loadExecutables(self):
        """ Loads  executables into Pilot-Data """
        
        if self._chunkDesc and self._chunkDesc.get('files', None):            
            desc = util.getEmptyDU(self._pilots[0]['pilot_compute'])
            desc['file_urls'] = self._chunkDesc['files']
            self._chunkExe = self.compute_data_service.submit_data_unit(desc)
            
        if self._mapDesc and self._mapDesc.get('files', None):            
            desc = util.getEmptyDU(self._pilots[0]['pilot_compute'])
            desc['file_urls'] = self._mapDesc['files']
            self._mapExe = self.compute_data_service.submit_data_unit(desc)
        
        if self._reduceDesc and self._reduceDesc.get('files', None):            
            desc = util.getEmptyDU(self._pilots[0]['pilot_compute'])
            desc['file_urls'] = self._reduceDesc['files']
            self._reduceExe = self.compute_data_service.submit_data_unit(desc)
        
        # Wait for the executable DUS
        util.waitDUs([self._chunkExe, self._mapExe, self._reduceExe])
        
    def _chunk(self): 
        """ Chunks input data if Chunk task is defined """
        
        if self._chunkDesc:               
            """ for each file in inputDU create a Chunk task """
            logger.debug("Chunking input data")
            chunkCUs = []
            try:
                for inputDu in self._inputDus:
                    temp = util.getEmptyDU(inputDu.data_unit_description)
                    temp = self.compute_data_service.submit_data_unit(temp)
                    temp.wait()
                    for fName in inputDu.list_files():
                        # for user defined ChunkDesc assign affinity.
                        self._chunkDesc = util.setAffinity(self._chunkDesc, inputDu.data_unit_description)
                        # Pass the input filename and output filename as arguments.
                        self._chunkDesc['arguments'] = [fName, "%s-%s" % (fName, constant.CHUNK_FILE_PREFIX)]
                        # Collect chunked files in output_du
                        self._chunkDesc['output_data'] = [ { temp.get_url(): ['*-chunk-*'] } ]
                        # Get input file to Chunk CU. 
                        self._chunkDesc["input_data"] = [ {inputDu.get_url(): [fName]} ] 
                        if self._chunkExe is not None:
                            self._chunkDesc["input_data"].append(self._chunkExe.get_url())
                                                            
                        chunkCUs.append(self.compute_data_service.submit_compute_unit(self._chunkDesc))
                    self._chunkDus.append(temp)
        
                # Wait for the chunk DUS    
                logger.debug("Wait for chunk DUS/CUS")            
                util.waitDUs(self._chunkDus)
                util.waitCUs(chunkCUs)
            except Exception, ex:
                self._clean(ex, "Chunk failed - Abort")
        else:
            logger.debug("Ignoring chunking of input data, as Chunk Description is not set for the MapReduce Job")
            

    def _map(self):
        """ Map Phase """
        
        # Create output DUS one for each reduce to collect all the Map Task results 
        logger.debug("Creating DUS to store Map Output results")
        for _ in range(self._nbrReduces):
            temp = util.getEmptyDU(self._pilots[0]['pilot_compute'])
            self.reduceDus.append(self.compute_data_service.submit_data_unit(temp))        
        util.waitDUs(self.reduceDus)
        
        pdString = "%s:%s" % (self.pdUrl.netloc,self.pdUrl.path)
        rduDirs = [os.path.join(pdString,rdu.get_url().split(":")[-1]) for rdu in self.reduceDus]
        rduString = ",".join(rduDirs)                        
        

        # Create task for each chunk in all the chunk data units
        
        mapCUs = []
        try:
            for cdu in self._chunkDus:
                for cfName in cdu.list_files():
                    mapTask = util.setAffinity(copy.copy(self._mapDesc), cdu.data_unit_description)
                    mapTask['arguments'] = [cfName, rduString] + self._mapDesc.get('arguments', [])
                    mapTask["input_data"] = [ {cdu.get_url(): [cfName]} ]                    
                    if self._iterDu:
                        mapTask["input_data"].append(self._iterDu.get_url())
                        for dui in self._iterDu.to_dict()["data_unit_items"]:
                            mapTask["arguments"].append(dui.__dict__["filename"])
                    if self._mapExe is not None:
                        mapTask["input_data"].append(self._mapExe.get_url())                        
                    mapCUs.append(self.compute_data_service.submit_compute_unit(mapTask))
        
            # Wait for the map DUS and CUS   
            logger.debug("Create & submitting Map tasks")             
            util.waitCUs(mapCUs)
        except Exception, ex:
            self._clean(ex, "Map Phase failed - Abort")                    

    def _reduce(self):
        """ Reduce Phase """
        
        logger.debug("Creating DUS to store Reduce Output results")
        # Create DU to collect output data of all the reduce tasks
        temp = util.getEmptyDU(self._pilots[0]['pilot_compute'])
        self._outputDu = self.compute_data_service.submit_data_unit(temp)                
        util.waitDUs([self._outputDu])

        # Create reduce for each reduce DU 
        reduceCUs = []        
        try:
            for rdu in self.reduceDus:
                mapOutPath=os.path.join(self.pdUrl.path,rdu.get_url().split(":")[-1])
                rduFiles = [os.path.join(mapOutPath,f) for f in os.listdir(mapOutPath)]                
                rdu.add_files(rduFiles)
                rdu.wait()                
                reduceTask = util.setAffinity(self._reduceDesc, rdu.data_unit_description)
                reduceTask['input_data'] = [rdu.get_url()]
                

                pdString = "%s:%s" % (self.pdUrl.netloc,self.pdUrl.path)
                outputDir = os.path.join(pdString,self._outputDu.get_url().split(":")[-1])            
                
                if self._iterOutputPrefixes:
                    reduceFiles = []
                    for pref in self._iterOutputPrefixes:
                        reduceFiles.append(pref+"*")
                else:
                    reduceFiles.append('reduce-*')

                reduceTask['arguments'] = [":".join(rdu.list_files()), outputDir, ",".join(reduceFiles)] + self._reduceDesc.get('arguments', [])

                    
                if self._reduceExe is not None:
                    reduceTask["input_data"].append(self._reduceExe.get_url())                    
                reduceCUs.append(self.compute_data_service.submit_compute_unit(reduceTask))
               
            # Wait for the map DUS and CUS 
            logger.debug("Create & submitting Reduce tasks")                
            util.waitCUs(reduceCUs)
        except Exception, ex:
            self._clean(ex, "Reduce Phase failed - Abort")                  

    def _collectOutput(self):
        """ Export Output DU to the user defined output path """

        reduceOutPath=os.path.join(self.pdUrl.path,self._outputDu.get_url().split(":")[-1])
        outFiles = [os.path.join(reduceOutPath,f) for f in os.listdir(reduceOutPath)]                
        self._outputDu.add_files(outFiles)        
        self._outputDu.export(self.outputPath)

        

    def getDetails(self):
        """ 
            Returns the execution time of MapReduce phases
        
            @return: dictionary with execution timing details of MapReduce phases
        
        """  
        raise NotImplementedError
    
    def chunkOnly(self, inputDu):
        """ 
            Executes the chunk Job only
        
            @param inputDu: Takes input Data Units as Input argument
            @return: List of chunk task output Data Units 
        
        """          
        self._inputDus = inputDu
        if self._inputDus:
            self._loadDataIntoPD()
            self._chunk()
            return self._chunkDus
        else:
            self.clean("Input DUS are invalid")
        
    
    def mapOnly(self, chunkDus):
        """ 
            Executes the Map Job only
        
            @param mapDus: Takes map chunk/split Data Units as Input
            @return: List of Map task output Data Units 
        
        """        
        self._chunkDus = chunkDus
        if self._chunkDus:
            self._map()
            return self._mapDus
        else:
            self.clean("Chunk DUS are invalid")
    
    def submitComputeUnit(self, desc):
        """ 
        Submits SAGA Job description to Pilot
        
        @param desc: SAGA Job description  
        """
        self.compute_data_service.submit_compute_unit(desc)
        
    def submitDataUnit(self, desc):
        """ 
        Submits SAGA Job description to Pilot
        
        @param desc: SAGA Job description  
        """
        tdu = self.compute_data_service.submit_data_unit(desc)
        tdu.wait()
        return tdu

    
    def reduceOnly(self, mapDus):
        """ 
            Executes the Reduce job only
        
            @param mapDus: Takes map task Data Units as Input
            @return: Output Data Unit 
        
        """
        
        self._mapDus = mapDus
        if self._mapDus:
            self._reduce()
            return self._outputDu
        else:
            self.clean("Map DUS are invalid")
            
    def mapReduce(self):
        self._loadDataIntoPD()
        self._chunk()
        self._map()
        self._reduce()
        self._collectOutput()                     

    def mapReduce(self):
        self._map()
        self._reduce()
        self._collectOutput() 
        
    def initialize(self):
        self._loadDataIntoPD()
        self._chunk()
        
    def setIterativeDataUnit(self, du):
        self._iterDu = du
        self.initializeIter()
        
    def initializeIter(self):   
        self._mapDus = []
        self.reduceDus = []
        self._outputDu = None              
        shutil.rmtree(self.outputPath, ignore_errors=True)    
    
    def setIterativeOutputPrefix(self, filePrefixes):
        self._iterOutputPrefixes = filePrefixes    
    
    def runJob(self):
        """ Executes the entire MapReduce workflow """
        self.startPilot()
        self._loadDataIntoPD()
        self._chunk()
        self._map()
        self._reduce()
        self._collectOutput()
        self.stopPilot()
