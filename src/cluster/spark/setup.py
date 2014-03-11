import os
import sys


def setup(nodes):
    # split node list
    nodeList = nodes.split(",")

    # Write to slaves
    fh = open("slaves","w")
    fh.write("\n".join(nodeList))
    fh.close()
    
    # Start Yarn
    os.system( "SPARK_MASTER_IP=%s SPARK_CONF_DIR=%s $SPARK_HOME/sbin/start-all.sh" % (nodeList[0], os.getcwd()) )  
    
if __name__ == "__main__":
    setup(sys.argv[1])          
