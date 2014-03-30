.. _chapter_yarnInstall:

******
Setup
******

Install
-------

Download and untar Yarn

.. code-block:: python

	cd $HOME
	wget http://apache.mirrors.spacedump.net/hadoop/common/stable/hadoop-2.2.0.tar.gz
	tar xvf hadoop-2.2.0.tar.gz --gzip
	rm hadoop-2.2.0.tar.gz # We no longer need the tar
	

Environment
-----------

Create a file .yarn.env in $HOME directory and source in .bashrc file, and place the below
contents in the .yarn.env file

.. code-block:: python

	export HADOOP_PREFIX=$HOME/hadoop-2.2.0
	export HADOOP_HOME=$HADOOP_PREFIX
	export HADOOP_COMMON_HOME=$HADOOP_PREFIX
	export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
	export HADOOP_HDFS_HOME=$HADOOP_PREFIX
	export HADOOP_MAPRED_HOME=$HADOOP_PREFIX
	export HADOOP_YARN_HOME=$HADOOP_PREFIX
	export PATH=$HADOOP_PREFIX/bin:$PATH
		
Comment `.hadoop.env` statement if exists in ~/.bashrc and `source ~/.bashrc` file

.. note:: Make sure, Java environment is already available.



Configuration
--------------

Edit $HADOOP_CONF_DIR/core-site.xml with below contents. The property `fs.defaultFS` should point to
`file://localhost`, since Pilot-Hadoop doesn't use HDFS as most of the current scientific clusters has
a shared file system. 

.. code-block:: xml

	<configuration>
	  <property>
	    <name>fs.defaultFS</name>
	    <value>file://localhost/</value>
	    <description>NameNode URI</description>
	  </property>
	</configuration>


Edit $HADOOP_CONF_DIR/yarn-site.xml with below contents. The property `yarn.resourcemanager.hostname` is mandatory 
and has variable `RESOURCE_MANAGER_HOSTNAME` as value, which is replaced later by one of the nodes acquired by Pilot. 

.. code-block:: xml

	<configuration>
	  <property>
	    <name>yarn.resourcemanager.hostname</name>
	    <value>RESOURCE_MANAGER_HOSTNAME</value>
	    <description>The hostname of the RM.</description>
	  </property>
	</configuration>
	
	
.. note:: You can also add other configuration properties related to the yarn-site.xml and core-site.xml
	