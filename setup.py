#!/usr/bin/env python

import os
from setuptools import setup


try:
    import saga
except:
    print "SAGA C++ and SAGA Python Bindings not found. Using Bliss/SAGA."
    #sys.exit(1)

fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
version = open(fn).read().strip()
    
setup(name='PilotMapReduce',
      version=version,
      description='SAGA Pilot-Abstractions based MapReduce Implementation',
      author='Pradeep Mantha',
      author_email='pmanth2@cct.lsu.edu',
      url='https://github.com/saga-project/PilotMapReduce',
      classifiers = ['Development Status :: 1 - Beta',                    
                    'Programming Language :: Python',
                    'Environment :: Console',                    
                    'Topic :: Utilities',
                    ],
      platforms = ('Unix', 'Linux', 'Mac OS'),
      packages=['pmr'],

      include_package_data=True,
      # data files for easy_install
      data_files = [('', ['README', 'README']), 
                    ('', ['VERSION', 'VERSION'])],
      
      # data files for pip
      install_requires=['bigjob']
)
