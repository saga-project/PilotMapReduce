import os

def stop():
    
    # Start Yarn
    os.system("HADOOP_CONF_DIR="+os.getcwd()+";$HADOOP_PREFIX/sbin/stop-yarn.sh")  

    
if __name__ == "__main__":
    stop()          

    