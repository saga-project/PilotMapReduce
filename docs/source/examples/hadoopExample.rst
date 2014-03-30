Example
=======

Below Pilot-Hadoop script, executes Apache Hadoop wordcount example. 
The script accepts the Pilot-Compute and Pilot-Data descriptions and request the compute resources.
Once the resources are acquired by pilot, Hadoop cluster is setup on the resources.
The Pilot executes the job, clean the cluster and terminates.

Hadoop Example
-------------------------
.. literalinclude:: ../../../examples/exampleHadoop.py    