###
### Example script/Application used to run mapreduce program
###

import sys
import time
import saga
import os
from optparse import OptionParser

# Import mapreduce library which is in current directory.
sys.path.append( os.getcwd() + "/../../")
print sys.path
from mapreduce import *


def main():
    
    parser = OptionParser()
    parser.add_option("-i", "--input", dest="input",
                  help="SAGA url of the input data location")
    parser.add_option("-o", "--output", dest="output_dir", 
                  help="SAGA url of the output data location")
    parser.add_option("-t", "--temp", dest="temp_dir", 
                  help="SAGA url of the temporary data location")
    parser.add_option("-b", "--reduces", dest="nbr_reduces", 
                  help="number of reduces")
    parser.add_option("-m", "--mapper", dest="mapper", 
                  help="physical complete path of mapper")
    parser.add_option("-r", "--reducer", dest="reducer", 
                  help="physical complete path of reducer")
    parser.add_option("-a", "--advert", dest="advert_host", 
                  help="advert host and port ex: redis://cyder.cct.lsu.edu:2525")
    parser.add_option("-u", "--resource", dest="resource_url", 
                  help="SAGA url of HPC resource. Could you gram/pbspro.")
    parser.add_option("-n", "--nodes", dest="number_nodes", 
                  help="Number of nodes requested for task execution")
    parser.add_option("-q", "--queue", dest="queue", 
                  help="Queue")
    parser.add_option("-y", "--allocation", dest="allocation", 
                  help="Allcoation")
    parser.add_option("-w", "--walltime", dest="walltime", 
                  help="Walltime")
    parser.add_option("-s", "--ppn", dest="ppn", 
                  help="processes_per_node")
    parser.add_option("-d", "--workingdirectory", dest="workingdirectory", 
                  help="Workingdirectory in which agent is executed and its log & error files are stored")
    parser.add_option("-x", "--userproxy", dest="userproxy", 
                  help="userproxy: currently use None")
    parser.add_option("-c", "--chunk", dest="chunk", 
                  help="chunk size")
    parser.add_option("-p", "--transfer", dest="transfer",default=None, 
                  help="transfer files to the target location ex : username@qb1.loni.org:target_directory ")
    parser.add_option("-e", "--npworkers", dest="npworkers",default=1, 
                  help="number of processes for each map/reduce function")

    (options, args) = parser.parse_args()
    print sys.argv
    
    print " **************************  Map Reduce Framework Started ************************* "
    initial_time = time.time()
    fsp = MapReduce(options.input,options.output_dir,options.temp_dir,options.nbr_reduces,options.mapper,options.reducer,options.advert_host,options.resource_url,options.number_nodes,options.queue, options.allocation, options.walltime, options.ppn, options.workingdirectory, options.userproxy, options.chunk, options.npworkers)
    print " \n >>> All Configuration parameters provided \n"
    starttime = time.time()
    
    fsp.chunk_input()
    runtime = time.time()-starttime
    print " \n Time taken to chunk : " + str(round(runtime,3)) +  "\n\n";
    starttime = time.time()
    fsp.map_job_submit()
    runtime = time.time()-starttime
    print " >>> Map Phase completed and it took ... " + str(round(runtime,3)) + "\n\n"
    starttime = time.time()
    fsp.reduce_job_submit()
    runtime = time.time()-starttime
    print " >>> Reduce Phase completed and it took ... " + str(round(runtime,3)) + "\n\n"  
    final_time = time.time() - initial_time
    print " >>> The total time taken is " + str(round(final_time,3)) + "\n\n"
    if options.transfer != None :
        print " " + saga.url(options.output_dir).path + " transfer" + options.transfer
        starttime = time.time()
        os.system("scp -r " + saga.url(options.output_dir).path + "/reduce* " + options.transfer )
        transfertime = time.time()-starttime
        print " >>> Transfer time " + str(round(transfertime,3)) + "\n\n"
        j="find " + saga.url(options.output_dir).path + " -name \"reduce-*\" -exec stat -c%s {} \; | awk '{total += $1} END {print total}'"
        k=os.popen(j).read()
        print str(float(k)/(1024*1024))
    sys.exit(0)
    
if __name__=="__main__":
    main()
