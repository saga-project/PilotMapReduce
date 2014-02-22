class mapper:
    def __init__(self, args):
        # the 1st argument is a  comma separated list of chunk files.                
        self.chunkFile = args[1]
        self.nbrReduces = int(args[2])
        
        # Used to store the keys and values 
        self.partitionFile = []
        self.partition = [[] for _ in range(self.nbrReduces)]
                
        # user defined map task arguments 
        self.mapArgs = args[3:]
        
    
    def emit(self, key, value):
        """ Emit the key value based on the partition function """ 
        self.partition[int(hash(key) % self.nbrReduces)].append(value)
        
    def finalize(self):
        """ Prepare the map output files 
            sort the map output contents """
            
        for i in range(0,self.nbrReduces):
            partitionName="partition-"+str(i) 
            self.partitionFile.append(open(self.chunkFile + "-sorted-map-"+ partitionName,'w'))

        for i in range(0,self.nbrReduces):
            self.partition[i].sort()
            for line in self.partition[i]:
                self.partitionFile[i].write(str(line)+"\n")
                

        for i in range(0,self.nbrReduces): 
            self.partitionFile[i].close()