.. _chapter_example_gettinstarted:

***************
Getting Started 
***************

** It is highly recommended that you carefully read and understand SAGA-Python and Pilot-Abstractions, Pilot-MapReduce 
all of this before you go off and start developing your own applications. **

In this chapter we explain the main components of Pilot-MapReduce and the 
foundations of their function and their interplay. For your convenience, you can find a 
fully working example at the end of this page.

.. note:: This chapter assumes that you have successfully installed Pilot-MapReduce on
          (see chapter :ref:`chapter_installation`).

This tutorial shows how to create and run a Pilot-MapReduce job that counts words. 
To start with, you need nothing but a input directory with a  single text file. 
Let’s call the file input.txt. If you don’t happen to have a suitable file on hand, 
you can download one from `here. <https://raw2.github.com/saga-project/PilotMapReduce/master/resources/data/wordcount/input.txt>`_
and save it in `$HOME/data` directory.


Loading the Module
------------------

In order to use Pilot-MapReduce in your Python application, you need to import the
``pmr`` module.

.. code-block:: python

    import pmr

You can check / print the version of your Pilot-MapReduce installation via the
``version`` property.

.. code-block:: python

    print pmr.version
    

Creating a MapReduce Job
------------------------
A :class:`pmr.MapReduce` is used to create MapReduce job in Pilot-MapReduce. It takes two parameters
as inputs:
 
    * List of dict objects with SAGA-Pilot compute, data descriptions and input data SAGA URL location.
    * Coordination system URL used by SAGA-Pilot abstractions for  managing communication between pilot-managers and pilots.
 
 .. code-block:: python

    pmrDesc = []    
    pmrDesc.append({
                    'pilot_compute': { "service_url": "fork://localhost",
                                      "number_of_processes": 8,
                                      "working_directory": os.getenv("HOME")+"/pilot-compute",   
                                      "affinity_datacenter_label": "eu-de-south",              
                                      "affinity_machine_label": "mymachine-1"                                
                                      },
                    'pilot_data'   :  {
                                      "service_url": "ssh://localhost/" + os.getenv("HOME")+"/pilot-data",
                                      "size": 100,   
                                      "affinity_datacenter_label": "eu-de-south",              
                                      "affinity_machine_label": "mymachine-1"                              
                                      },
                    'input_url'    : 'sftp://localhost/'+ os.getenv("HOME") + "/data")
                    })
    
    job = pmr.MapReduce(pmrDesc, COORDINATION_URL)
    
    
Create Mapper executable
------------------------
Create a Mapper executable in a new file `wc_mapper.py` in your $HOME directory using your favorite editor.

The map executable has mainly 3 sections

* `Initialization` : Each map task receives chunk/split files as command line arguments. Initialize map task with command line parameters.
    
 .. code-block:: python
 
    mapJob = Mapper(sys.argv)
    
* `Map function` : Actual map task implementation. The :class:`pmr.Mapper` provides a member `chunkFile` to access the chunk file passed as input to the map task. It also provide a member function `emit` which emits key, value pair to the reduce phase.

 .. code-block:: python

    with open(mapJob.chunkFile) as fh:        
        line = fh.read()
        for word in line.split():
            mapJob.emit(word, "%s,%s" % (word, 1))

    
* `Finalization` : Cleanup of the task.
    
 .. code-block:: python

        mapJob.finalize()

After putting it all together, your first Pilot-MapReduce Mapper executable will look somewhat 
like the script below.        

.. literalinclude:: ../../../applications/wordcount/wc_mapper.py


     
Create Reducer executable
-------------------------
Create a Reducer executable in a new file `wc_reducer.py` in your $HOME directory using your favorite editor.

The reduce executable also has mainly 3 sections

* `Initialization` : Each Reduce task receives list of sorted partition files as command line arguments. Initialize reduce task with command line parameters.

.. code-block:: python

    reduceJob = Reducer(sys.argv)

* `Reduce function` : Actual Reduce task implementation. The :class:`pmr.Reducer` provides a member `partitionFiles` to access list of partition files passed as input to the reduce task. It also provide a member function `emit` which emits key, value pair to the output result file.

.. code-block:: python

    count = {}        
    # split the map emitted to get words count from each partition file                       
    for pName in reduceJob.partitionFiles:
        with open(pName) as infile:
            for line in infile:
                tokens = line.split(",")
                
                # Actual word might contain "," and count is last token. 
                value = int(tokens[-1])
                word = ",".join(tokens[:-1])
                
                if count.has_key(word):
                    count[word] = count[word] + value
                else:
                    count[word] = value
    for word, count in count.iteritems():                
        reduceJob.emit(word, count)

* `Finalization` : Cleanup of the task.

.. code-block:: python

    reduceJob.finalize() 

After putting it all together, your first Pilot-MapReduce Reducer executable will look somewhat 
like the script below.        

.. literalinclude:: ../../../applications/wordcount/wc_reducer.py
    
 
Define Chunk, Map, Reduce jobs  
------------------------------
Define Chunk, Map and Reduce tasks as dict representation of `SAGA Job description attributes <http://saga-project.github.io/saga-python/doc/library/job/index.html#job-description-saga-job-description>`_

.. code-block:: python

    chunkDesc  = { "executable": "split -l 50" }
    
    mapDesc    = { "executable": "python wc_mapper.py",
                   "number_of_processes": 1,
                   "spmd_variation":"single",
                   "files" : ['ssh://localhost/' + os.getenv("HOME") + "/wc_mapper.py")]
                 }
    
    reduceDesc = { "executable": "python wc_reducer.py",              
                   "number_of_processes": 1,
                   "spmd_variation":"single",
                   "files" : ['ssh://localhost/' + os.getenv("HOME") + "/wc_reducer.py")]
                 }
                 
.. note:: Attribute `files` is not a SAGA Job description attribute. This attribute takes a list of files to be transferred
	to the Chunk/Map/Reduce task execution working directory even in a distributed Pilot-MapReduce case.                 



Register the Chunk, Mapper, Reduce Job description to MapRedue Job
------------------------------------------------------------------

.. code-block:: python

    job.setChunk(chunkDesc)
    job.setMapper(mapDesc)
    job.setReducer(reduceDesc)
    
Set the number of reducers and the output path
-----------------------------------------------    

.. code-block:: python

    job.setNbrReduces(8)
    job.setOutputPath(os.getenv("HOME")+"/output")

Submit the Job
---------------

.. code-block:: python

    job.runJob()
    
The Complete Example
--------------------

After putting it all together, your first Pilot-MapReduce application will look somewhat 
like the script below.

.. literalinclude:: ../../../examples/tutorial.py    
    
