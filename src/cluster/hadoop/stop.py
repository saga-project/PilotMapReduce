import os

def stop():
    
    # Stop MapReduce
    os.system("HADOOP_CONF_DIR="+os.getcwd()+";$HADOOP_PREFIX/bin/stop-mapred.sh")  

    
if __name__ == "__main__":
    stop()          