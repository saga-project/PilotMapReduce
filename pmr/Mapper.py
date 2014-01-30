class Mapper:
    def __init__(self, args):
        self.chunkFileName=args[1]
        self.nbrReduces = int(args[2])
        self.sortedPartitionNbr = []
        self.partitionNbr = [[] for _ in range(self.nbrReduces)]
        self.mapArgs = args[3:]
    
    def emit(self, key, value):
        self.partitionNbr[key % int(self.nbrReduces)].append(value)
        
    def finalize(self):
        for i in range(0,self.nbrReduces):
            partitionName="part-"+str(i) 
            self.sortedPartitionNbr.append(open(self.chunkFileName + "-sorted-map-"+ partitionName,'w'))

        for i in range(0,self.nbrReduces):
            self.partitionNbr[i].sort()
            for l in self.partitionNbr[i]:
                self.sortedPartitionNbr[i].write(l) 

        for i in range(0,self.nbrReduces): 
            self.sortedPartitionNbr[i].close()