from collections import defaultdict
import uuid

import trio


class TaskNode(object):
    """
    Node representing task to be executed.

    args:
        graph (TaskGraph):       Graph object that the task is scheduled within
        task_handle (str):       Name of function that will be executed
        task_arguments (tuple):  Tuple of function arguments in the form: (args, kwargs)
        status (str):            Initial task status
    """
    def __init__(self, graph, task_handle, task_arguments, initial_status=None):

        self.graph = graph
        self.uid = 'task-' + uuid.uuid4().hex
        self.edges_by_label = defaultdict(list)

        self.task_handle = task_handle
        assert self.task_handle in graph.expected_functions
        self.task_arguments = task_arguments

        self.task_status = initial_status or 'ready_for_execution'
        self.task_result = None
        self.exception_trace = None

        self.notify_lock = trio.Lock()
        self.task_completed_event = trio.Event()

    def __repr__(self):
        return 'TaskNode: %s %s' % (self.task_handle, self.uid)

    @property
    def edges_by_labelO(self):
        # copy of self.edges_by_label with the uids resolved to
        # their objects, this is convenient when debugging
        nodes = self.graph.nodes
        conditions = self.graph.conditions
        di = defaultdict(list)
        for label, uids in self.edges_by_label.items():
            for uid in uids:
                if uid.startswith('condition-'):
                    di[label].append(conditions[uid])
                elif uid.startswith('task-'):
                    di[label].append(nodes[uid])
        return di

    def add_edge(self, node_or_uid, label):
        uid = node_or_uid if type(node_or_uid) is str else node_or_uid.uid
        self.edges_by_label[label].append(uid)

    async def notify_predecessor_condition_satisfied(self):

        if self.task_status == 'precondition_failed':
            # all pre-conditions must succeed so subsequent successes are irrelevant
            return

        conditions = self.graph.conditions
        async with self.notify_lock:
            condition_statuses = [
                conditions[uid].condition_status for uid in self.edges_by_label['waits_on']
            ]
            if set(condition_statuses) != {'satisfied'}:
                return  # do nothing

            # schedule this task for execution
            self.task_status = 'ready_for_execution'
            await self.graph.task_send_channel.send(self.uid)

    async def notify_predecessor_condition_failed(self):
        self.task_status = 'precondition_failed'

    def get_predecessor_task_nodes(self):
        # get all task_nodes from all prior conditions of this task_node
        nodes_by_uid = self.graph.nodes
        conditions_by_uid = self.graph.conditions

        task_nodes = []
        task_nodes_by_name = defaultdict(list)

        for condition_uid in self.edges_by_label['waits_on']:
            for task_uid in conditions_by_uid[condition_uid].edges_by_label['waits_on']:
                node_obj = nodes_by_uid[task_uid]

                task_nodes_by_name[node_obj.task_handle].append(node_obj)
                task_nodes.append(node_obj)

        return task_nodes, task_nodes_by_name

    def get_successors(self, task_handle):
        nodes_by_uid = self.graph.nodes
        conditions_by_uid = self.graph.conditions

        task_tuples = []

        for condition_uid in self.edges_by_label['waited_on_by']:
            for task_uid in conditions_by_uid[condition_uid].edges_by_label['waited_on_by']:
                node_obj = nodes_by_uid[task_uid]
                if node_obj.task_handle != task_handle:
                    continue
                task_tuples.append(
                    (nodes_by_uid[task_uid], conditions_by_uid[condition_uid])
                )

        return task_tuples

    def prepend_to_successor(self, successor_task_handle, new_task):
        # sometimes a task needs to add a new task as a predecessor to its successor, thereby
        # postponing it successor's execution
        for successor_node, successor_condition in self.get_successors(successor_task_handle):
            successor_condition.edges_by_label['waits_on'].append(new_task.uid)
            new_task.edges_by_label['waited_on_by'].append(successor_condition.uid)
