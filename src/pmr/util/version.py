#pylint: disable=C0301, C0103, W0212, E1101, R0903

"""
.. module:: pmr.utils.version
   :platform: Unix
   :synopsis: Implementation of the version extractor.

.. moduleauthor:: Pradeep Mantha <pradeepm66@gmail.com>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os

version = open(os.path.dirname (os.path.abspath (__file__)) + "/../VERSION", 'r').read().strip()