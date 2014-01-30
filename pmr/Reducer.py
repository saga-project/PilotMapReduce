class Reducer:
    def __init__(self, args):
        self.partitionFiles=args[1]
        self.partitionFiles=self.partitionFiles.split(":")
        k=str(self.partitionFiles[0].split("-")[-1:][0])
        reduceFile="reduce-"+str(k) 
        self.reduceWrite=open(reduceFile, 'w')   
    
    def emit(self, key, value):
        self.reduceWrite.write("%s,%s" % (key,value)) 
        
    def finalize(self):  
        self.reduceWrite.close()    