import os

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
        self.reduceDirs = args[2].split(",")       
        self.nbrReduces = len(self.reduceDirs)
        
        # Used to store the keys and values 
        self.partitionFile = []
        self.partitionList = [[] for _ in range(self.nbrReduces)]
                
        # user defined map task arguments 
        self.mapArgs = args[3:]
    
    def partition(self, key):
        """ 
        Default partition function. This function could be overwritten by custom
        partition functions.
        
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

        for i in range(0, self.nbrReduces): 
            self.partitionFile[i].close()
        
        for i in range(0, self.nbrReduces):    
            fname =   self.chunkFile + "-sorted-map-partition-" + str(i)     
            scp_cmd = "scp -r %s %s" %(fname, self.reduceDirs[i])
            print "Moving output file via cmd : %s" % scp_cmd
            ret=os.system(scp_cmd)
            if ret == 0:
                print "File successfully transferred"
                try:
                    os.remove(fname)
                except:
                    pass
            else:
                print "File transfer failed"
            
             
            
