#!/usr/bin/env python

__author__ = "Pradeep Mantha"
__copyright__ = "Copyright 2011, Pradeep Mantha"
__license__ = "MIT"


from pmr import MapReduce, util
from pmr.util.logger import logger
import os


class Yarn(MapReduce):
    
    """
        Hadoop: Class for managing Yarn Jobs
    
    """ 
      
     
    def __init__(self,pmrDesc, coordinationUrl):
        """ 
            Initializes MapReduce with Pilot computes/Data description and
            coordination system 
            
        """
        logger.info("Initialize PMR-Yarn")
        MapReduce.__init__(self, pmrDesc, coordinationUrl)
        self._setupScript = os.path.join(os.path.dirname(__file__), "../cluster/yarn/setup.py") 
        self._stopScript = os.path.join(os.path.dirname(__file__), "../cluster/yarn/stop.py")
        
             
    def setUpCluster(self):
        """ 
            Setup Yarn Cluster 
        """
        
        self.startPilot()
        pcs = self.getPilotComputes()
        
        logger.info("Setup Yarn Cluster")
        i=0        
        hadoopSetupTasks =[]       
        for pilot in pcs:
            setUpTask = {}                        
            desc = util.getEmptyDU(self._pilots[i]['pilot_compute'])
            self._pilotInfo[i]['hadoopConfDir'] = self.compute_data_service.submit_data_unit(desc)
            setUpTask = util.setAffinity(setUpTask, self._pilotInfo[i]['hadoopConfDir'].data_unit_description)
            setUpTask['output_data'] = [
                                         {
                                          self._pilotInfo[i]['hadoopConfDir'].get_url(): ['yarn-site.xml','core-site.xml','slaves']
                                         }
                                       ]
            setUpTask['executable'] = "python"
            nodes = pilot.get_nodes()
            setUpTask['arguments'] = [self._setupScript, ",".join(nodes)]
            
            hadoopSetupTasks.append(self.compute_data_service.submit_compute_unit(setUpTask))            
            i=i+1        
        util.waitCUs(hadoopSetupTasks)
        logger.info("Cluster ready")
        
    
    def submitJob(self,desc):
        
        """ Submit Yarn Job description """
        
        logger.info("Submitting Yarn Jobs")
        hadoopTasks =[]
        i=0
        for pilot in self._pilots:
            task = {} 
            task.update(desc)                       
            task = util.setAffinity(task, pilot['pilot_compute'])
            task['executable'] = 'HADOOP_CONF_DIR=$PWD;' + task['executable']
            task['input_data'] = [self._pilotInfo[i]['hadoopConfDir'].get_url()]            
            hadoopTasks.append(self.compute_data_service.submit_compute_unit(task))            
            i=i+1
        util.waitCUs(hadoopTasks)            
        return hadoopTasks    
        
    
    def stopCluster(self):
        """ Tear down Yarn cluster """
        
        logger.info("Stopping Yarn Cluster")
        hadoopStopTasks =[]
        i=0
        for pilot in self._pilots:
            stopTask = {}                        
            setUpTask = util.setAffinity(stopTask, pilot['pilot_compute'])
            setUpTask['executable'] = "python"
            setUpTask['input_data'] = [self._pilotInfo[i]['hadoopConfDir'].get_url()]
            setUpTask['arguments'] = [self._stopScript]
            
            hadoopStopTasks.append(self.compute_data_service.submit_compute_unit(setUpTask))            
            i=i+1        
        util.waitCUs(hadoopStopTasks)
        self.stopPilot()
