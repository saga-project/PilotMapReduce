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
        self.reduceDir = args[2]      
        self.nbrReduces = int(args[3])
        
        # Used to store the keys and values 
        self.partitionFile = []
        self.partitionList = [[] for _ in range(self.nbrReduces)]
                
        # user defined map task arguments 
        self.mapArgs = args[4:]
    
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
            
        i=0    
        for partition in self.partitionList:
            if len(partition) > 0:
                partitionName = "partition-" + str(i)
                partition.sort()
                f=open(self.chunkFile + "-sorted-map-" + partitionName, 'w')
                f.writelines("%s\n" % item for item in partition)
                f.close()
            i=i+1
        
        fname =   self.chunkFile + "-sorted-map-partition-*"              
        if "localhost" in self.reduceDir:
            transfer_cmd = "ln -s %s/%s %s" %(os.getcwd(),fname, self.reduceDir.split(":")[1])
        else:
            transfer_cmd = "scp -r %s %s" %(fname, self.reduceDir)
        print "Moving output file via cmd : %s" % transfer_cmd
        ret=os.system(transfer_cmd)

        if ret == 0:
            print "File successfully transferred"
            try:
                os.system("rm -fr %" % fname)
            except:
                pass
        else:
            print "File transfer failed"
            
             
            
