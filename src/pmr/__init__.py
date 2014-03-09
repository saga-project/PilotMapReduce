"""
.. module:: Pilot-MapReduce
   :platform: Unix
   :synopsis:  A Pilot based MapReduce framework.

.. moduleauthor:: Pradeep Mantha <pradeepm66@gmail.com>
"""

__license__   = "MIT"

# ------------------------------------------------------------------------------
#
from pmr.mapreduce                   import MapReduce 
from pmr.mapper                      import Mapper
from pmr.reducer                     import Reducer  
from pmr.hadoop                      import Hadoop     
# ------------------------------------------------------------------------------
from pmr.util.version             import version
from pmr.util.logger              import logger
