import os
import glob

class Reducer:
    """
        reducer: Class for managing Reduce phase of MapReduce Job
    
    """     
    def __init__(self, args):
        """ 
            Initializes Reduce task with parameters passed by MapReduce framework
            as command line parameters.
            
        """
        self.partitionFiles=glob.glob("*-sorted-map-partition-*")            
        self.reduce=str(self.partitionFiles[0].split("-")[-1])
        reduceFile="reduce-"+str(self.reduce)
        self.outputDir = args[1]
        self.reduceOutFiles = args[2].split(",")
        self.reduceArgs = args[3:]
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
           
        for fname in self.reduceOutFiles:    
            if "localhost" in self.outputDir:
                transfer_cmd = "ln -s %s/%s %s" %(os.getcwd(),fname, self.outputDir.split(":")[1])
            else:
                transfer_cmd = "scp -r %s %s" %(fname, self.outputDir)
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
        