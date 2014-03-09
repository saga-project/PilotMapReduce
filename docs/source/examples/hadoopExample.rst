Example
=======

Below Pilot-Hadoop script, executes Apache Hadoop 2.2.0 wordcount example. 
The script accepts the Pilot-Compute and Pilot-Data descriptions and request the compute resources.
Once the resources are acquired by pilot, Hadoop cluster is setup on the resources.
The Pilot executes the hadoop job, clean the cluster and terminates.

The Complete Example
--------------------

After putting it all together, your first Pilot-MapReduce application will look somewhat 
like the script below.

.. literalinclude:: ../../../examples/wcHadoop.py    
