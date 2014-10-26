class Reducer:
    """
        reducer: Class for managing Reduce phase of MapReduce Job
    
    """     
    def __init__(self, args):
        """ 
            Initializes Reduce task with parameters passed by MapReduce framework
            as command line parameters.
            
        """
        self.partitionFiles=args[1]
        self.partitionFiles=self.partitionFiles.split(":")        
        self.reduce=str(self.partitionFiles[0].split("-")[-1])
        reduceFile="reduce-"+str(self.reduce) 
        self.reduceWrite=open(reduceFile, 'w')   
    
    def emit(self, key, value):
        """ Emit the key value pair to reduce file """
        
        if key is None:
            self.reduceWrite.write("%s" % value)
        else:
            self.reduceWrite.write("%s,%s" % (key,value)) 
        
    def finalize(self):  
        """ Close the reduce file """
        self.reduceWrite.close()    