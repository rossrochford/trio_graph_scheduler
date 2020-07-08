# Graph-based Task Scheduler for Trio

Task scheduler for the Trio concurrency framework, similar to Directed Acyclic Graphs (DAGs), although cycles aren't strictly prevented.

The purpose of this is to:

* Separate task scheduling and concurrency logic from the core functionality of the tasks.
* Provide a shared scheduling data structure that can be updated dynamically during execution.
* Provide a temporary in-memory store of task results within a graph.
* Provide constraints on task concurrency by defining separate worker-loop groups for different task types.


### Usage:

```
# abbreviated code, for full example see: trio_graph_scheduler/examples/example1.py

import trio

from trio_graph_scheduler.execution import execute_graph
from trio_graph_scheduler.graph import TaskGraph


async def get_word_counts(**kwargs):
    
    # get results of predecessors
    predecessors = kwargs['task_node'].get_predecessor_task_nodes()
    
    url = predecessors[0].task_arguments[0][0] 
    page_source = predecessors[0].task_result

    # omitted: compute work_counts

    return url, word_counts


async def main():
    
    # tell the TaskGraph which functions/names to expect
    FUNCTIONS = {
        'get_page_source': get_page_source,  # omitted, see tests/example.py
        'get_word_counts': get_word_counts,
        'combine_page_word_counts': combine_page_word_counts,  
    }
    # define the worker loops, their concurrency and expected functions
    WORKER_LOOPS = [
        WorkerLoop('default', 3, None),
        WorkerLoop('html_loop', 4, ['get_page_source'])
    ]

    graph = TaskGraph(FUNCTIONS, WORKER_LOOPS)

    word_count__task_uids = []

    for url in page_urls:
        ps_task = await graph.create_task(
            # (function_name, arguments, predecessor_tasks)
            'get_page_source', ((url,), {}), None
        )
        wc_task = await graph.create_task(
            # no arguments, get_word_counts() will fetch its inputs from the results of its predecessor tasks
            'get_word_counts', None, [ps_task.uid]
        )
        word_count__task_uids.append(wc_task.uid)
    
    # a SchedulingCondition object becomes 'satisfied' when some criteria is true, a generic COMPLETE__ALL condition entails waiting 
    # for all tasks in 'word_count__task_uids' to complete (successfully or not). Once satisfied, tasks waiting on it are scheduled.
    wait_condition = GenericSchedulingCondition(
        graph, 'COMPLETE__ALL', word_count__task_uids
    )

    # this task waits for 'wait_condition' to be satisfied
    await graph.create_task(
        'combine_page_word_counts', None, [wait_condition.uid]
    )
    
    # execute this graph, tasks will get execute concurrently 
    # based on the active worker loops
    await execute_graph(graph) 


if __name__ == '__main__':
    trio.run(main)
```


To run this example do:

```
# cd into repository
$ cd trio_graph_scheduler/

# create and activate virtualenv
$ python3 -m venv venv/
$ source venv/bin/activate
$ pip install -r requirements.txt
$ export PYTHONPATH=$(pwd)

# run example
$ python trio_graph_scheduler/examples/example1.py
```

Once execution is complete, it will render a graph of the TaskNodes and SchedulingConditions, like this:

A task graph looks something like this:

![Image](https://i.ibb.co/KWxLxyt/example-graph.png)

The yellow node is the root Graph object, the square nodes are TaskNodes and the circular nodes are SchedulingCondition objects. The entire network is green which means all Tasks were successful and all SchedulingConditions were satisfied.

