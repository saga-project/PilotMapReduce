class Mapper:
    """
        mapper: Class for managing Map phase of MapReduce Job
    
    """ 
    def __init__(self, args):
        """ 
            Initializes Map task with parameters passed by MapReduce framework
            as command line parameters.
            
        """
        # the 1st argument is a  comma separated list of chunk files.                
        self.chunkFile = args[1]
        self.nbrReduces = int(args[2])
        
        # Used to store the keys and values 
        self.partitionFile = []
        self.partitionList = [[] for _ in range(self.nbrReduces)]
                
        # user defined map task arguments 
        self.mapArgs = args[3:]
    
    def partition(self, key):
        """ 
        Default partition function
        
        @param key: map task emitted key value
        @return: partition number into which the key,value pair has to be written to  
        """
        return int(hash(key) % self.nbrReduces)
        
    
    def emit(self, key, value):
        """ Emit the key value based on the partition function """ 
        self.partitionList[self.partition(key)].append(value)
        
    def finalize(self):
        """ Prepare the map output files 
            sort the map output contents """

        # open partition file for each reduce            
        for i in range(0, self.nbrReduces):
            partitionName = "partition-" + str(i) 
            self.partitionFile.append(open(self.chunkFile + "-sorted-map-" + partitionName, 'w'))

        # sort each partition  list and write to file
        for i in range(0, self.nbrReduces):
            self.partitionList[i].sort()
            for line in self.partitionList[i]:
                self.partitionFile[i].write(str(line) + "\n")
        
        # close all partition files
        for i in range(0, self.nbrReduces): 
            self.partitionFile[i].close()
