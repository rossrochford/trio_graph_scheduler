# Graph-based Task Scheduler for Trio

Task scheduler for the Trio concurrency framework, similar to Directed Acyclic Graphs (DAGs), although cycles aren't strictly prevented.

The purpose of this is to:

* Separate task scheduling/dependency from the functionality/logic of tasks.
* Provide a shared scheduling data structure that can change dynamically during execution.
* Provide a temporary in-memory store of task results for a graph.
* Provide constraints on task concurrency by assinging task-types to worker-loop groups.

This is very much a work in progress and the api is a little inelegant. To see how it's used, take a look at: tests/example.py

# Future work
* Expose this as a REST API so other services can get the state of a task graph, fetch results or push new tasks to the graph. 
* Add hierarchical logging so graph execution can be traced and task log lines consolidated.
* Add multi-process support?
