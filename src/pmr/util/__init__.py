import os
import time

import saga
import saga.filesystem

from pilot import DataUnit
from pmr.util.logger import logger



class constant(object):
    JOB_COMPLETION_STATUS = ['Done','Failed']
    DU_OR_PILOT_COMPLETION_STATUS = ['Running','Failed']
    CHUNK_FILE_PREFIX='`hostname`-chunk-'
    MAP_PARTITION_FILE_REGEX = '*-sorted-map-partition-'
    


def getEmptyDU(unit):
    """ returns empty DU with affinity values of given unit"""
    return { "file_urls": [],
             "affinity_datacenter_label": unit['affinity_datacenter_label'],
             "affinity_machine_label": unit['affinity_machine_label']}  
def setAffinity(unita, unitb):
    """ set affinity of target unit to the source  unit """
    unita['affinity_datacenter_label'] = unitb['affinity_datacenter_label']
    unita['affinity_machine_label'] = unitb['affinity_machine_label']
    return unita
    
    
    
def getFileUrls(inputUrl, fileTransferType):
        """ Create list of file urls used to create DU's """ 
        inputUrl=saga.Url(inputUrl)         
        inputDir = saga.filesystem.Directory(inputUrl) 
        inputUrl.scheme = fileTransferType          
        return map(lambda i:os.path.join(str(inputUrl),str(i)), inputDir.list())   
    
def getDuUrl(input_url):
    # Connect to existing DU
    return DataUnit(du_url = input_url)   


def waitCUs(jobs):
    _wait(jobs, constant.JOB_COMPLETION_STATUS )


def waitDUs(dus):
    _wait(dus, constant.DU_OR_PILOT_COMPLETION_STATUS )

def waitPilots(pilots):
    _wait(pilots, constant.DU_OR_PILOT_COMPLETION_STATUS )
    
            
def _wait(units , states):
    """ Wait for the untils until they are completed/failed """
    count={}

    units = filter(lambda i: i!=None, units)
    if len(units) > 0:
        while(True):
            uStates = map(lambda i: i.get_state(), units) 
            allStates = set(uStates)
            for s in states:
                count[s] = 0
            completeJobs = 0
            
            for s in allStates:                
                if s in states:
                    count[s] = uStates.count(s)
                    completeJobs = completeJobs + count[s]
                count['Other'] = len(units)-completeJobs
                    
            logger.debug(count)            
        
            if count['Other'] > 0:
                time.sleep(2)
            else:
                break
