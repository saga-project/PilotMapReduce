.. _chapter_intro:

************
Introduction
************

Pilot-MapReduce (PMR) is a Pilot-based implementation of the MapReduce programming model. 
By decoupling job scheduling and monitoring from the resource management using Pilot-based abstraction, 
PMR can efficiently re-use the resource management and late-binding capabilities of PilotJob and PilotData. 
PMR exposes an easy-to-use interface, which provides the complete functionality needed by any MapReduce algorithm, 
while hiding the more complex functionality, such as chunking of the input, sorting the intermediate results, 
managing and coordinating the map & reduce tasks, etc., which are implemented by the framework.

.. image:: images/architecture.pdf
