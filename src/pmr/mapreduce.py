#!/usr/bin/env python

__author__ = "Pradeep Mantha"
__copyright__ = "Copyright 2011, Pradeep Mantha"
__license__ = "MIT"


from pilot import PilotComputeService, ComputeDataService, PilotDataService
from pmr import util
from pmr.util import constant
from pmr.util.logger import logger


class MapReduce(object):
    
    """
        MapReduce: Class for managing MapReduce Jobs
    
    """ 
      
     
    def __init__(self, pmrDesc, coordinationUrl):
        """ 
            Initializes MapReduce with Pilot computes/Data description and
            coordination system 
            
        """
            
        logger.info("Initialize Pilot-MapReduce")
        
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

        self._pilotInfo = [{}] * len(self._pilots)
        
        
        self.compute_data_service = None
        self.pilot_compute_service = None
        self.pilot_data_service = None

    def startPilot(self):
        """ Start the pilot compute and data services """
        
        logger.info("Start pilot service")
        try:
            self.compute_data_service = ComputeDataService()    
            self.pilot_compute_service = PilotComputeService(self._coordinationUrl)
            self.pilot_data_service = PilotDataService(self._coordinationUrl)  
            self._startPilotComputeDatas()
        except Exception, ex:
            self._clean(ex, "Pilot service initialization failed - abort")
    
    
        
    def stopPilot(self):
        """ Stops the pilot compute and data services """
        logger.info("Terminate pilot Service")
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
        self._outputPath = path
        
        
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

        logger.info("Loading input data into Pilot-Data")
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
        
        logger.info("New Pilot-MapReduce descriptions with updated PD URLS \n"  \
                    "use these descriptions to reuse already uploaded data")
        map(lambda x: logger.info(x), self._pilots)
        
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
            logger.info("Chunking input data")
            chunkCUs = []
            try:
                for inputDu in self._inputDus:
                    temp = util.getEmptyDU(inputDu.data_unit_description)
                    temp = self.compute_data_service.submit_data_unit(temp)
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
            logger.info("Ignoring chunking of input data, as Chunk Description is not set for the MapReduce Job")
            

    def _map(self):
        """ Map Phase """
        
        # Create output DUS one for each reduce to collect all the Map Task results 
        logger.debug("Creating DUS to store Map Output results")
        for _ in range(self._nbrReduces):
            temp = util.getEmptyDU(self._pilots[0]['pilot_compute'])
            self.reduceDus.append(self.compute_data_service.submit_data_unit(temp))        
        util.waitDUs(self.reduceDus)

        # Create task for each chunk in all the chunk data units
        
        mapCUs = []
        try:
            for cdu in self._chunkDus:
                for cfName in cdu.list_files():
                    mapTask = util.setAffinity(self._mapDesc, cdu.data_unit_description)
                    mapTask['arguments'] = [cfName, self._nbrReduces] + self._mapDesc.get('arguments', [])
                    mapTask['output_data'] = []
                    for i in range(self._nbrReduces):
                        mapTask['output_data'].append({ self.reduceDus[i].get_url(): [constant.MAP_PARTITION_FILE_REGEX + str(i)] })
                    mapTask["input_data"] = [ {cdu.get_url(): [cfName]} ] + self._mapDesc["input_data"]
                    if self._mapExe is not None:
                        mapTask["input_data"].append(self._mapExe.get_url())
                    mapCUs.append(self.compute_data_service.submit_compute_unit(mapTask))
        
            # Wait for the map DUS and CUS   
            logger.info("Create & submitting Map tasks")             
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
                reduceTask = util.setAffinity(self._reduceDesc, rdu.data_unit_description)
                reduceTask['arguments'] = [":".join(rdu.list_files())] + self._reduceDesc.get('arguments', [])
                reduceTask['input_data'] = [rdu.get_url()]
                reduceTask['output_data'] = [{self._outputDu.get_url(): ['reduce-*'] }] 
                if self._mapExe is not None:
                    reduceTask["input_data"].append(self._reduceExe.get_url())
                reduceCUs.append(self.compute_data_service.submit_compute_unit(reduceTask))
               
            # Wait for the map DUS and CUS 
            logger.info("Create & submitting Reduce tasks")                
            util.waitCUs(reduceCUs)
        except Exception, ex:
            self._clean(ex, "Reduce Phase failed - Abort")                  

    def _collectOutput(self):
        """ Export Output DU to the user defined output path """
        
        self._outputDu.export(self._outputPath)

        

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
        return self.compute_data_service.submit_compute_unit(desc)
        
    
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
            
    def mapReduceOnly(self):
        self._loadDataIntoPD()
        self._chunk()
        self._map()
        self._reduce()
        self._collectOutput()                     
        
    
    def runJob(self):
        """ Executes the entire MapReduce workflow """
        self.startPilot()
        self._loadDataIntoPD()
        self._chunk()
        self._map()
        self._reduce()
        self._collectOutput()
        self.stopPilot()
        
    def runKmeansIterativeJob(self, seed, nbrIterations=1):
        """ Executes MapReduce iterative """
        
        self.startPilot()
        self._loadDataIntoPD()
        self._chunk()
        iteration = 1
                
        while iteration <= nbrIterations:            
            self._iterDU = self.compute_data_service.submit_data_unit(seed)
            util.waitDUs(self._iterDU)
            
            self._mapDesc["input_data"] = [self._iterDU.get_url()] 
            self._map()
            self._reduce()
            self._collectOutput()
            
            # merge reduce outputs in self._outputPath
            newCenterFile = self._outputPath+'/centers.txt-'+str(iteration)
            with open(newCenterFile, 'w') as mergeFile:
                for centerOut in glob.glob( '%s/%s*' % (self._outputPath, )):
                    logger.info("processing file %s " % centerOut)
                    centerRead = open(os.path.join(self._outputPath,centerOut),'r')
                    for line in centerRead:
                        mergeFile.write(line)
                    centerRead.close()    

            seed["file_urls"] =  newCenterFile                   
            iteration = iteration + 1                            
            
            
               

            
            
            
            
            
            

        
        
        
        
        
        
