import os
import sys

def stop(nodes):
    
    # split node list
    nodeList = nodes.split(",")
    
    # Start Yarn
    os.system( "SPARK_MASTER_IP=%s SPARK_CONF_DIR=%s $SPARK_HOME/sbin/stop-all.sh" % (nodeList[0], os.getcwd()) )  

    
if __name__ == "__main__":
    stop(sys.argv[1])          

    