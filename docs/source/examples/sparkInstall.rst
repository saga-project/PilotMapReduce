.. _chapter_sparkInstall:

******
Setup
******

Install
-------

Download and untar Spark

.. code-block:: python

	cd $HOME
	wget http://d3kbcqa49mib13.cloudfront.net/spark-0.9.0-incubating-bin-hadoop2.tgz
	tar xvf spark-0.9.0-incubating-bin-hadoop2.tgz --gzip
	mv spark-0.9.0-incubating-bin-hadoop2 spark-0.9.0
	rm spark-0.9.0-incubating-bin-hadoop2.tgz # We no longer need the tar
	

Environment
-----------

Create a file .spark.env in $HOME directory and source in .bashrc file, and place the below
contents in the ~/.spark.env file

.. code-block:: python

	export SPARK_HOME=$HOME/spark-0.9.0
	export PATH=$SPARK_HOME/bin/:$PATH
		
`source ~/.bashrc` file

.. note:: Make sure, Java environment is already available.


Configuration
--------------

Edit `$SPARK_HOME/sbin/spark-config.sh and comment line exporting environment variable SPARK_CONF_DIR.
The Pilot framework  configures SPARK_CONF_DIR dynamically to set the master and worker nodes. 
	
.. note:: You can also add other configuration properties related to the Spark
	