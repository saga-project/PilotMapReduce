class Reducer:
    def __init__(self, args):
        self.partitionFiles=args[1]
        self.partitionFiles=self.partitionFiles.split(":")        
        self.reduce=str(self.partitionFiles[0].split("-")[-1:][0])
        reduceFile="reduce-"+str(self.reduce) 
        self.reduceWrite=open(reduceFile, 'w')   
    
    def emit(self, key, value):
        if key is None:
            self.reduceWrite.write("%s" % value)
        else:
            self.reduceWrite.write("%s,%s" % (key,value)) 
        
    def finalize(self):  
        self.reduceWrite.close()    