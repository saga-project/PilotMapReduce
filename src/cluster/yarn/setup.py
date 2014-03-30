import os
import sys


def setup(nodes):
    # split node list
    nodeList = nodes.split(",")
    
    # Copy conf files to current directory
    os.system("cp " + os.getenv("HADOOP_CONF_DIR") + "/* .")
    
    # Open yarn-site.xml and replace RESOURCE_MANAGER_HOSTNAME with first token
    # of sys.argv
    fh = open("yarn-site.xml")
    fc = fh.read()
    fh.close()
    
    fc=fc.replace("RESOURCE_MANAGER_HOSTNAME",nodeList[0])
    
    fh = open("yarn-site.xml","w")
    fh.write(fc)
    fh.close()
    
    
    # Write to slaves
    fh = open("slaves","w")
    if len(nodeList) > 1:
        fh.write("\n".join(nodeList))
    else:
        fh.write(nodeList[0])
    fh.close()
    
    # Start Yarn
    os.system("HADOOP_CONF_DIR="+os.getcwd()+ ";$HADOOP_PREFIX/sbin/start-yarn.sh")  
    
if __name__ == "__main__":
    setup(sys.argv[1])          
