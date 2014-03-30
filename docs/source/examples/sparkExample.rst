Example
=======

Below Pilot-Spark script, executes Apache Spark 0.9.0 wordcount/pi calculation example. 
The script accepts the Pilot-Compute and Pilot-Data descriptions and request the compute resources.
Once the resources are acquired by pilot, Spark cluster is setup on the resources.
The Pilot executes the spark job, clean the cluster and terminates.

The Complete Example
--------------------

After putting it all together, your first Pilot-Spark application will look somewhat 
like the script below.

.. literalinclude:: ../../../examples/exampleSpark.py    
