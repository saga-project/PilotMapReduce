class PMRDescription(dict):
    """ B{PMRDescription}
        
        A PMRDescription is a based on the attributes defined on 
        the SAGA Pilot Compute and Data Description.

        The PMRDescription is used by the application to specify 
        what kind of PilotCompute and PilotData it requires.
        
        Example::
             pmrDescription = {
                                'pilot_compute': SAGA PilotCompute description,
                                'pilot_data': SAGA PilotData description 
                                'input_url': Input data SAGA URL location / redis
                              }
                 
    """

    # Class members
    __slots__ = (
        # Pilot Compute/Data description
        'pilot_compute',
        'pilot_data',
        # Input Data SAGA URL
        'input',
    )

   
    def __init__(self):
        pass
    
    
    def __setattr__(self, attr, value):
        self[attr]=value
        
    
    def __getattr__(self, attr):
        return self[attr]
    
    
class MapReduce(object):
    def __init__(self, p=None,coordinationUrl):
        pass
    
    def setNbrReduces(self):
        raise NotImplementedError    
        
    def startPilot(self):
        raise NotImplementedError    
            
    def setMapper(self):
        raise NotImplementedError
        
    def setReducer(self):
        raise NotImplementedError
        
    def setPartitioner(self):
        raise NotImplementedError
    
    def setChunk(self):
        raise NotImplementedError
    
    def setOutputPath(self):
        raise NotImplementedError
    
    def stopPilot(self):
        raise NotImplementedError
    
    def _map(self):
        raise NotImplementedError
    
    def _shuffle(self):
        raise NotImplementedError    
        
    def _reduce(self):
        raise NotImplementedError
    
    def collectOutput(self):
        raise NotImplementedError

    def getDetails(self):
        raise NotImplementedError

    def mapOnly(self):
        raise NotImplementedError                 
        
    
    def runJob(self):
        raise NotImplementedError                   
    