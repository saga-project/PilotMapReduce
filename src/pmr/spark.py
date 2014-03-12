#!/usr/bin/env python

__author__ = "Pradeep Mantha"
__copyright__ = "Copyright 2011, Pradeep Mantha"
__license__ = "MIT"


from pmr import MapReduce, util
from pmr.util.logger import logger
import os


class Spark(MapReduce):
    
    """
        Spark: Class for managing Spark cluster
    
    """ 
      
     
    def __init__(self, pmrDesc, coordinationUrl):
        """ 
            Initializes MapReduce with Pilot computes/Data description and
            coordination system 
            
        """
        logger.info("Initialize PMR-Spark")
        MapReduce.__init__(self, pmrDesc, coordinationUrl)
        sparkPath = os.path.join(os.path.dirname(__file__), "../cluster/spark")
        self._setupScript = os.path.join(sparkPath + "/setup.py") 
        self._stopScript = os.path.join(sparkPath + "/stop.py")
        self.nodes = None
        
             
    def setUpCluster(self):
        """ 
            Setup Spark Cluster 
        """
        
        self.startPilot()
        pcs = self.getPilotComputes()
        
        logger.info("Setup Spark Cluster")
        i=0        
        sparkSetupTasks =[]       
        for pilot in pcs:
            setUpTask = {}                        
            desc = util.getEmptyDU(self._pilots[i]['pilot_compute'])
            self._pilotInfo[i]['sparkConfDir'] = self.compute_data_service.submit_data_unit(desc)
            setUpTask = util.setAffinity(setUpTask, self._pilotInfo[i]['sparkConfDir'].data_unit_description)
            setUpTask['output_data'] = [
                                         {
                                          self._pilotInfo[i]['sparkConfDir'].get_url(): ['slaves']
                                         }
                                       ]
            setUpTask['executable'] = "python"
            self.nodes = pilot.get_nodes()
            setUpTask['arguments'] = [self._setupScript, ",".join(self.nodes)]
            
            sparkSetupTasks.append(self.compute_data_service.submit_compute_unit(setUpTask))            
            i=i+1        
        util.waitCUs(sparkSetupTasks)
        logger.info("Cluster ready")
    
    def getSparkMaster(self):   
        return self.nodes[0] 
    
    def submitJob(self,desc):
        
        """ Submit spark Job description """
        logger.info("Submitting Spark Job")
        sparkTasks =[]
        i=0
        for pilot in self._pilots:
            task = {} 
            task.update(desc)                       
            task = util.setAffinity(task, pilot['pilot_compute'])
            task['executable'] = 'SPARK_CONF_DIR=$PWD;' + task['executable']
            task['input_data'] = [self._pilotInfo[i]['sparkConfDir'].get_url()]            
            sparkTasks.append(self.compute_data_service.submit_compute_unit(task))            
            i=i+1            
        return sparkTasks   
     
        
    
    def stopCluster(self):
        """ Tear down spark cluster """
        
        logger.info("Stopping spark Cluster")
        sparkStopTasks =[]
        i=0
        for pilot in self._pilots:                    
            stopTask = util.setAffinity({}, pilot['pilot_compute'])
            stopTask['executable'] = "python"
            stopTask['input_data'] = [self._pilotInfo[i]['sparkConfDir'].get_url()]
            stopTask['arguments'] = [self._stopScript,  ",".join(self.nodes)]
            
            sparkStopTasks.append(self.compute_data_service.submit_compute_unit(stopTask))            
            i=i+1        
        util.waitCUs(sparkStopTasks)
        self.stopPilot()
