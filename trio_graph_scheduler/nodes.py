from collections import defaultdict
import uuid

import trio


class Node(object):

    def __init__(self, graph):
        self.graph = graph
        self.uid = uuid.uuid4().hex
        self.edges_by_label = defaultdict(list)
        # add node to graph
        self.graph.nodes[self.uid] = self

        # depth is set by creation or execution function on a best-effort basis
        # used for visualising log messages hierarchically
        self.depth = None

    @property
    def prev(self):
        # convenience prop, used for calculating depth
        if self.edges_by_label['scheduled_by']:
            return self.graph.nodes[self.edges_by_label['scheduled_by'][0]]
        if self.edges_by_label['waits_on']:
            return self.graph.nodes[self.edges_by_label['waits_on'][0]]
        return None

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

    args:
        graph (TaskGraph):       Graph object that the task is scheduled within
        task_handle (str):       Name of function that will be executed
        task_arguments (tuple):  Tuple of function arguments in the form: (args, kwargs)
        status (str):            Initial task status
    """
    def __init__(self, graph, task_handle, task_arguments, status='ready_for_execution'):

        super(TaskNode, self).__init__(graph)

        self.task_handle = task_handle
        assert self.task_handle in graph.expected_functions
        self.task_arguments = task_arguments

        self.task_status = status
        self.task_result = None
        self.exception_trace = None
        self.completion_event = trio.Event()

    def _get_extra_kwargs(self):
        return {'task_node': self, 'graph': self.graph}


# ready if all finished with no errors
# ready if all finished but may have errors
# ready if N predecessors completed successfully
# ready if N predecessors completed (successfully or not)

# SUCCESS__ALL
# COMPLETE__ALL
# SUCCESS__N
# COMPLETE__N

IN_PROGRESS_STATUSES = (
    'ready_for_execution', 'in_progress', 'waiting_for_predecessors'
)


STATUS_SHORTCODES = {
    'success': 'S',
    'cancelled': 'C',
    'in_progress': 'IP',
    'waiting_for_predecessors': 'WFP'
}


def _is_ready__success(wait_task, number_or_all):

    predecessor_statuses = wait_task.predecessor_statuses

    if set(predecessor_statuses) == {'failed'}:
        import pdb; pdb.set_trace()

    if number_or_all == 'ALL':

        if 'failed' in predecessor_statuses:
            wait_task.task_status = 'predecessor_failed'
            return False

        for in_progress_status in IN_PROGRESS_STATUSES:
            if in_progress_status in predecessor_statuses:
                return False
        '''
        log_prefixlog_prefix = ''
        if wait_task.depth is not None:
            log_prefix = '     ' * wait_task.depth
        else:
            import pdb; pdb.set_trace()
        status_log_li = [STATUS_SHORTCODES.get(status, status) for status in predecessor_statuses]
        print(log_prefix + 'WAIT_TASK COMPLETE (SUCCESS__ALL), predecessors: %s' % status_log_li)
        '''
        return True

    threshold = int(number_or_all)
    num_successful = predecessor_statuses.count('success')
    if num_successful >= threshold:
        return True

    return False


def _is_ready__complete(wait_task, number_or_all):

    predecessor_statuses = wait_task.predecessor_statuses

    if set(predecessor_statuses) == {'failed'}:
        import pdb; pdb.set_trace()

    if number_or_all == 'ALL':
        for in_progress_status in IN_PROGRESS_STATUSES:
            if in_progress_status in predecessor_statuses:
                return False
        '''
        log_prefix = ''
        if wait_task.depth:
            log_prefix = '     ' * wait_task.depth
        status_log_li = [STATUS_SHORTCODES.get(status, status) for status in predecessor_statuses]
        print(log_prefix + 'WAIT_TASK COMPLETE (COMPLETE__ALL), predecessors: %s' % status_log_li)
        '''
        return True

    threshold = int(number_or_all)
    num_complete = predecessor_statuses.count('success') + predecessor_statuses.count('failed')
    if num_complete >= threshold:
        return True

    return False


def _is_ready(wait_task):

    success_or_complete, number_or_all = wait_task.schedule_mode.split('__')
    assert success_or_complete in ('SUCCESS', 'COMPLETE')

    if success_or_complete == 'SUCCESS':
        return _is_ready__success(wait_task, number_or_all)

    return _is_ready__complete(wait_task, number_or_all)


class WaitTaskNode(TaskNode):
    """
    Node representing a deferred task to be executed
    when all its 'waits_on' predecessors are complete

    args:
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
    def __init__(self, graph, task_handle, task_arguments, schedule_mode):
        super(WaitTaskNode, self).__init__(graph, task_handle, task_arguments)

        self.task_status = 'waiting_for_predecessors'
        self.schedule_mode = schedule_mode
        self.notify_lock = trio.Lock()
        assert '__' in schedule_mode

    def _get_extra_kwargs(self):
        extra_kwargs = super(WaitTaskNode, self)._get_extra_kwargs()
        predecessor_nodes = [
            self.graph.nodes[uid] for uid in self.edges_by_label['waits_on']
        ]
        extra_kwargs.update(
            {'predecessor_nodes': predecessor_nodes}
        )
        return extra_kwargs

    @property
    def predecessor_statuses(self):
        statuses = []
        for uid in self.edges_by_label['waits_on']:
            predecessor_status = self.graph.nodes[uid].task_status
            statuses.append(predecessor_status)
        return statuses

    def is_ready(self):
        return _is_ready(self)

    '''
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
    '''

    async def notify_predecessor_complete(self):
        if self.task_status != 'waiting_for_predecessors':
            return  # no need to do anything

        async with self.notify_lock:
            if self.is_ready():
                self.task_status = 'ready_for_execution'
                await self.graph.task_send_channel.send(self.uid)


class SubTask(TaskNode):

    @property
    def parent(self):
        parent_uid = self.edges_by_label['parent_group_task'][0]
        return self.graph.nodes[parent_uid]

    def _get_extra_kwargs(self):
        extra_kwargs = super(SubTask, self)._get_extra_kwargs()
        extra_kwargs['parent_group_task'] = self.parent
        return extra_kwargs

    async def wait_for_completion(self, log=False):
        # convenience method for surrounding wait with log lines
        if log is False:
            await self.completion_event.wait()
            return

        pending_subtasks = [
            n for n in self.parent.subtasks
            if n.task_status == 'waiting_for_parent_TaskGroupThresholdNode'
        ]

        log_prefix = ''
        if self.depth is not None:
            log_prefix = '     ' * self.depth

        print(log_prefix + 'waiting for sub-task: %s (remaining: %s)' % (
            self.uid[:6], len(pending_subtasks)
        ))

        await self.completion_event.wait()

        print(log_prefix + 'sub-task complete: %s' % self.uid[:6])


class NodeQuerySet(list):

    def filter(self, handle=None, status=None):

        assert handle or status
        li = self

        if handle:
            li = [n for n in li if n.task_handle == handle]
        if status:
            li = [n for n in li if n.task_status == status]

        return NodeQuerySet(li)


class TaskGroupThresholdNode(Node):
    """
    Represents a group of sub-tasks where N must complete successfully in order
    for the group to be marked completed with task_status = 'success'

    Generally you will add N sub-tasks, where N >= target. Its execution
    behaviour is to schedule 'target' sub-tasks for execution, wait and
    if any of these fail, schedule additional tasks.
    """
    def __init__(
        self, graph, target_num_successful,
        min_num_successful=None, status='ready_for_execution'
    ):
        super(TaskGroupThresholdNode, self).__init__(graph)
        self.target_num_successful = target_num_successful

        if min_num_successful is not None:
            # if this is set, it will attempt to his target but
            # will acceptable this minimum
            assert target_num_successful >= min_num_successful
        self.min_num_successful = min_num_successful

        self.task_status = status
        self.completion_event = trio.Event()

    async def create_subtask(self, task_handle, task_arguments):

        status = 'waiting_for_parent_TaskGroupThresholdNode'
        subtask_node = SubTask(self.graph, task_handle, task_arguments, status)

        if subtask_node.depth is None and self.depth is not None:
            subtask_node.depth = self.depth + 1

        self.add_edge(subtask_node, 'sub_task')
        subtask_node.add_edge(self, 'parent_group_task')

        return subtask_node.uid, subtask_node

    @property
    def subtasks(self):
        nodes = self.graph.nodes
        return NodeQuerySet([
            nodes[uid] for uid in self.edges_by_label['sub_task']
        ])

    def is_successful(self):
        successful_tasks = [n for n in self.subtasks.filter(status='success')]
        if self.min_num_successful is None:
            target_num = self.target_num_successful
        else:
            target_num = self.min_num_successful
        if len(successful_tasks) >= target_num:
            return True
        return False
