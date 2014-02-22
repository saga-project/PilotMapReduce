#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Pradeep Mantha"
__copyright__ = "Copyright 2011, Pradeep Mantha"
__license__   = "MIT"


import logging

from pilot import PilotComputeService, ComputeDataService, PilotDataService, \
    DataUnit, State
from pmr import util
from pmr.util import constant
import sys

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger('PMR')


class MapReduce(object):
    
    """
        MapReduce: Class for managing MapReduce Jobs
    
    """ 
      
     
    def __init__(self, pmrDesc,coordinationUrl):
        """ 
            Initializes MapReduce with Pilot computes/Data description and
            coordination system        
            
        """
            
        logger.info("Initialize Pilot-MapReduce")
        self._pilots = pmrDesc
        self.coordinationUrl = coordinationUrl
        
        self._inputDus=[]
        
        self._chunkDus=[]
        self._chunkDesc = None
        self._chunkExe = None
        
        self._mapDus=[]
        self._mapDesc = None
        self._mapExe = None
        
        self.reduceDus=[]
        self._reduceDesc = None
        self._reduceExe = None

        self.nbrReduces=1        
        self._outputDu = None
             
        self.pdFTP = "ssh"
        
    
    def startPilot(self):
        """ Start the pilot compute and data services """
        
        logger.info("Start pilot service")
        try:
            self.compute_data_service=ComputeDataService()    
            self.pilot_compute_service=PilotComputeService(self.coordinationUrl)
            self.pilot_data_service=PilotDataService(self.coordinationUrl)  
            self._startPilotComputeDatas()
        except:
            self._clean("Pilot service initialization failed - abort")
    
        
    def stopPilot(self):
        logger.info("Terminate pilot Service")
        try:
            self.compute_data_service.cancel()    
            self.pilot_compute_service.cancel()
            self.pilot_data_service.cancel()
        except:
            raise Exception ("Pilot service termination failed - abort")          
        
    def setNbrReduces(self, nbrReduces):
        self.nbrReduces=nbrReduces
        
    def setChunk(self, chunkDesc):
        self._chunkDesc = chunkDesc
                
    def setMapper(self, mapDesc):
        self._mapDesc = mapDesc
        
    def setReducer(self,reduceDesc):
        self._reduceDesc = reduceDesc

    def setOutputPath(self, path):
        self._outputPath = path
        
        
        
    def _clean(self,msg):
        """ Stops  the pilot compute and data services """

        self.stopPilot()
        raise Exception(msg)
                
    def _startPilotComputeDatas(self):
        def create(pilot):
            self.pilot_compute_service.create_pilot(pilot['pilot_compute'])
            self.pilot_data_service.create_pilot(pilot['pilot_data'])
            
        map(create, self._pilots)
            
        self.compute_data_service.add_pilot_compute_service(self.pilot_compute_service)
        self.compute_data_service.add_pilot_data_service(self.pilot_data_service)    
                  
        
    def _loadDataIntoPD(self):
        logger.debug("Loading input data into Pilot-Data")
        try:
            self._loadInputData()
            self._loadExecutables()
        except:
            self._clean("Loading input data failed - abort")
            


    def _loadInputData(self):        
        for pilot in self._pilots:
            if pilot['input_url'].startswith('redis'):
                # reconnect to Pilot-Data
                self._inputDus.append(util.getDuUrl(pilot['input_url']))                
            else:      
                desc = util.getEmptyDU(pilot['pilot_compute'])
                desc['file_urls'] = util.getFileUrls(pilot['input_url'], self.pdFTP)
                temp = self.compute_data_service.submit_data_unit(desc)
                pilot['input_url'] = temp.get_url()
                self._inputDus.append(temp)
        util.waitDUs(self._inputDus)
        
        logger.info("New Pilot-MapReduce descriptions with updated PD URLS \n"  \
                    "use these descriptions to reuse already uploaded data")
        map(lambda x: logger.info(x), self._pilots)
        
    def _loadExecutables(self):
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
        """ for each file in inputDU create a Chunk task """
        chunkCUs=[]
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
                    self._chunkDesc['output_data'] =  [ { temp.get_url(): ['*-chunk-*'] } ]
                    # Get input file to Chunk CU. 
                    self._chunkDesc["input_data"] = [ {inputDu.get_url(): [fName]} ] 
                    if self._chunkExe is not None:
                        self._chunkDesc["input_data"].append(self._chunkExe.get_url())
                                                        
                    chunkCUs.append(self.compute_data_service.submit_compute_unit(self._chunkDesc))
                self._chunkDus.append(temp)
    
            # Wait for the chunk DUS                
            util.waitDUs(self._chunkDus)
            util.waitCUs(chunkCUs)
        except:
            self._clean("Chunk failed - Abort")
            

    def _map(self):
        
        for _ in range(self.nbrReduces):
            temp = util.getEmptyDU(self._pilots[0]['pilot_compute'])
            self.reduceDus.append(self.compute_data_service.submit_data_unit(temp))        
        util.waitDUs(self.reduceDus)

        mapCUs = []
        try:
            for cdu in self._chunkDus:
                for cfName in cdu.list_files():
                    mapTask = util.setAffinity(self._mapDesc, cdu.data_unit_description)
                    mapTask['arguments'] = [cfName,self.nbrReduces] + self._mapDesc.get('arguments',[])
                    mapTask['output_data'] =[]
                    for i in range(self.nbrReduces):
                        mapTask['output_data'].append({ self.reduceDus[i].get_url(): [constant.MAP_PARTITION_FILE_REGEX + str(i)] })
                    mapTask["input_data"] = [ {cdu.get_url(): [cfName]} ] 
                    if self._mapExe is not None:
                        mapTask["input_data"].append(self._mapExe.get_url())
                    mapCUs.append(self.compute_data_service.submit_compute_unit(mapTask))
        
            # Wait for the map DUS and CUS                
            util.waitCUs(mapCUs)
        except:
            self._clean("Map Phase failed - Abort")                    

    def _reduce(self):
        reduceCUs = []
        temp = util.getEmptyDU(self._pilots[0]['pilot_compute'])
        self._outputDu = self.compute_data_service.submit_data_unit(temp)        
        
        util.waitDUs([self._outputDu])
        
        try:
            for rdu in self.reduceDus:
                reduceTask = util.setAffinity(self._reduceDesc, rdu.data_unit_description)
                reduceTask['arguments'] = [":".join(rdu.list_files())] + self._reduceDesc.get('arguments',[])
                reduceTask['input_data'] = [rdu.get_url()]
                reduceTask['output_data'] = [{self._outputDu.get_url(): ['reduce-*'] }]
                if self._mapExe is not None:
                    reduceTask["input_data"].append(self._reduceExe.get_url())
                reduceCUs.append(self.compute_data_service.submit_compute_unit(reduceTask))
               
            # Wait for the map DUS and CUS                
            util.waitCUs(reduceCUs)
        except:
            self._clean("Reduce Phase failed - Abort")                  

    def _collectOutput(self):
        self._outputDu.export(self._outputPath)

        
    def setPartitioner(self):
        raise NotImplementedError
        

    def getDetails(self):
        raise NotImplementedError
    
    def chunkOnly(self, inputDu):
        self._inputDus = inputDu
        if self._inputDus:
            self._loadDataIntoPD()
            self._chunk()
            return self._chunkDus
        else:
            self.clean("Input DUS are invalid")
        
    
    def mapOnly(self, chunkDus):
        self._chunkDus = chunkDus
        if self._chunkDus:
            self._map()
            return self._mapDus
        else:
            self.clean("Chunk DUS are invalid")
    
    
    def reduceOnly(self, mapDus):
        self._mapDus = mapDus
        if self._mapDus:
            self._reduce()
            return self._outputDu
        else:
            self.clean("Map DUS are invalid")
            
    
    def runJob(self):
        self.startPilot()
        self._loadDataIntoPD()
        self._chunk()
        self._map()
        self._reduce()
        self._collectOutput()
        self.stopPilot()