from collections import defaultdict
import uuid

import trio


class Node(object):

    def __init__(self, graph):
        self.graph = graph
        self.graph_uid = graph.uid
        self.uid = uuid.uuid4().hex
        self.edges_by_label = defaultdict(list)

        # add node to graph
        self.graph.nodes[self.uid] = self

    @property
    def edges_by_label_taskhandle(self):
        di = defaultdict(list)
        for label, node_uids in self.edges_by_label.items():
            for uid in node_uids:
                task_handle = self.graph[uid].task_handle
                di[(label, task_handle)].append(uid)
        return di

    def add_edge(self, node, label):
        self.edges_by_label[label].append(node.uid)


class TaskNode(Node):
    """
    Node representing task to be executed.

    Args:
        graph (TaskGraph):      Graph object that the task is scheduled within
        task_handle (str):      Name of function that will be executed
        task_arguments (tuple): Tuple of function arguments in the form: (args, kwargs)
        status (str):           Initial task status
    """
    def __init__(self, graph, task_handle, task_arguments, status='ready_for_execution'):

        super(TaskNode, self).__init__(graph)

        self.task_handle = task_handle
        assert self.task_handle in graph.expected_functions
        self.task_arguments = task_arguments

        self.task_status = status
        self.task_result = None
        self.exception_trace = None

    def _get_extra_kwargs(self):
        return {'task_node': self, 'graph': self.graph}


class WaitTaskNode(TaskNode):
    """
    Node representing a deferred task to be executed
    when all its 'waits_on' predecessors are complete

    Args:
        graph (TaskGraph):                      Graph object that the task is scheduled within
        task_handle (str):                      Name of function that will be executed

        task_arguments (tuple/function/None):   Tuple of function arguments in the form: (args, kwargs)
                                                or: None, in which case args are set to list 'predecessors'
                                                or: a function that takes predecessors list and returns arguments tuple
                                                - usually a function is used when arguments aren't available until
                                                  predecessors have finished executing or if you'd rather write
                                                  a get_args() function than a wrapper function for the task

        schedule_when_errors_found (bool):      Whether all predecessors need to be be successful for this
                                                task to be executed.
    """

    def __init__(self, graph, task_handle, task_arguments, schedule_when_errors_found):
        super(WaitTaskNode, self).__init__(graph, task_handle, task_arguments)

        self.task_status = 'waiting_for_predecessors'
        self.schedule_when_errors_found = schedule_when_errors_found
        self.notify_lock = trio.Lock()

    def _get_extra_kwargs(self):
        extra_kwargs = super(WaitTaskNode, self)._get_extra_kwargs()
        predecessor_nodes = [
            self.graph.nodes[uid] for uid in self.edges_by_label['waits_on']
        ]
        extra_kwargs.update(
            {'predecessor_nodes': predecessor_nodes}
        )
        return extra_kwargs

    def is_ready(self):

        if self.task_status == 'predecessor_failed':
            return False

        statuses = []
        for uid in self.edges_by_label['waits_on']:
            predecessor_status = self.graph.nodes[uid].task_status
            statuses.append(predecessor_status)
            if predecessor_status in ('ready_for_execution', 'in_progress', 'waiting_for_predecessors'):
                return False

            if self.schedule_when_errors_found is False:
                if predecessor_status == 'failed':
                    self.task_status = 'predecessor_failed'
                    return False

        if set(statuses) == {'failed'}:
            import pdb; pdb.set_trace()
        return True

    async def notify_predecessor_complete(self):
        if self.task_status != 'waiting_for_predecessors':
            return  # no need to do anything

        async with self.notify_lock:
            if self.is_ready():
                self.task_status = 'ready_for_execution'
                await self.graph.task_send_channel.send(self.uid)
