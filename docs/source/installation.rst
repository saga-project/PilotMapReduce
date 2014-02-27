
.. _chapter_installation:

************
Installation
************

Requirements 
============

Pilot-MapReduce relies on a set of external software packages, all of which get 
installed automatically as dependencies. 


* Python >= 2.7

* BigJob (https://pypi.python.org/pypi/BigJob)

Installation
============

Currently Pilot-MapReduce can only be installed via pip directly from GitHub. 

To install Pilot-MapReduce from the stable (main) branch in a virtual environment, 
open a terminal and run:

.. code-block:: bash

    virtualenv $HOME/myenv
    source $HOME/myenv/bin/activate
    pip install --upgrade -e git://github.com/saga-project/PilotMapReduce.git@master#egg=PilotMapReduce

Next, you can do a quick sanity check to make sure that the the packages have
been installed properly. In the same virtualenv, run:

.. code-block:: bash

    pmr-version

This should return the version of the Pilot-MapReduce installation, e.g., `0.X.Y`.

Installation from Source
========================

If you are planning to contribute to the Pilot-MapReduce codebase, you can download
and install Pilot-MapReduce directly from the sources.

First, you need to check out the sources from GitHub.

.. code-block:: bash

    git@github.com:saga-project/PilotMapReduce.git

Next, run the installer directly from the source directory (assuming you have 
set up a vritualenv):

.. code-block:: bash
 
    python setup.py install

Optionally, you can try to run the unit tests:
# Not developed yet

.. code-block:: bash

    python setup.py test 

.. note:: More on testing can be found in chapter :ref:`chapter_testing`.