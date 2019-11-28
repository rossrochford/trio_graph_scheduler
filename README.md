# Graph-based Task Scheduler for Trio

This is a task scheduler similar to a Directed Acyclic Graph (though cycles aren't prevented) for async tasks executed with the Trio concurrency framework. 

The purpose of this is to:

* Separate task scheduling/dependency from the functionality/logic of tasks.
* Provide a shared scheduling data structure that can change dynamically during execution. 
* Provide constraints on task concurrency by defining groups of worker loops.
