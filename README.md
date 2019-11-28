# Graph-based Task Scheduler for Trio

This is a task scheduler similar to a Directed Acyclic Graph (though cycles aren't prevented) for async tasks executed with the Trio concurrency framework. 

The purpose of this is to:

* Separate task scheduling/dependency from the functionality/logic of tasks.
* Provide a shared scheduling data structure that can change dynamically during execution and act as an in-memory store of task results.
* Provide constraints on task concurrency by assinging task-types to worker-loop groups.


This is very much a work in progress and the api is a little inelegant. To see how it's used, take a look at: tests/example.py
