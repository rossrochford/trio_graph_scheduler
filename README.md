# Graph-based Task Scheduler for Trio

Task scheduler for the Trio concurrency framework, similar to Directed Acyclic Graphs (DAGs), although cycles aren't strictly prevented.

The purpose of this is to:

* Separate task scheduling/dependency from the functionality/logic of tasks.
* Provide a shared scheduling data structure that can change dynamically during execution.
* Provide a temporary in-memory store of task results within a graph.
* Provide constraints on task concurrency by assinging task-types to worker-loop groups.

This is very much a work in progress and the api is a little inelegant.

A task graph looks something like this:

![Image](https://i.ibb.co/WcsFR7Z/tmp-exmple-graph.png)

### Usage:

```
import trio

from trio_graph_scheduler.execution import execute_graph
from trio_graph_scheduler.graph import TaskGraph

TASK_FUNCTIONS = {
    # name -> function
    'get_html': get_html,
    'process_html': process_html
}

# define worker loops
WORKER_LOOPS = {
    'default': 4,            # 'default' is a fallback for unassigned tasks
    'http_worker_loop': 10   # e.g. max concurrency for HTTP tasks
}

WORKER_ASSIGNMENTS = {
    'get_html': 'http_worker_loop'
}


async def main():
    # create graph 
    graph = TaskGraph(TASK_FUNCTIONS, WORKER_LOOPS, WORKER_ASSIGNMENTS)
    
    URLS = [
        'https://trio.readthedocs.io/en/stable/',
        'https://docs.python.org/3/whatsnew/3.8.html'
    ]
    
    # add task nodes to graph
    for url in URLS:
        html_task_uid = await graph.create_task(
            'get_html', ((url,), {}), graph.root.uid
        )

        # args is set to None here, see docstring for more info
        uid = await graph.create_wait_task(            
            'get_word_counts', None, [html_task_uid], True
        )
    
    await execute_graph(graph, 3) 


if __name__ == '__main__':
    trio.run(main)
```

For a more complex example see: tests/example.py


## Future work
* Expose this as a REST API so other services can get the state of a task graph, fetch results or push new tasks to the graph. 
* Add hierarchical logging so graph execution can be traced and task log lines consolidated.
* Add multi-process support?
