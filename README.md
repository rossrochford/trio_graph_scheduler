# trio_graph_scheduler

Graph-based task scheduler for async tasks executed with Trio. 

The purpose of this is to:
  -Separate task scheduling/dependency from the functionality/logic of tasks.
  -Provide a shared scheduling data structure that can change dynamically during execution. 
  -Provide constraints on task concurrency by defining groups of worker loops.
