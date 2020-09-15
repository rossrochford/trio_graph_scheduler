from collections import defaultdict, namedtuple
import uuid

from trio_graph_scheduler.util import draw_graph__pyvis
from trio_graph_scheduler.nodes import TaskNode
from trio_graph_scheduler.scheduling import GenericSchedulingCondition

'''
scheduling flow
-----------------

Task -> Condition
    - task notifies condition that it has completed
    - condition checks whether its criteria is true

Condition -> Task
    - condition notifies next tasks (via edge 'waited_on_by') 

'''


class TaskGraph(object):

    WorkerLoop = namedtuple('WorkerLoop', 'name concurrency functions')

    def __init__(self, functions, worker_loops, worker_context_init_functions=None):

        worker_loop_names = [obj.name for obj in worker_loops]
        assert 'default' in worker_loop_names

        self.uid = 'graph-' + uuid.uuid4().hex

        self.expected_functions = functions
        self.worker_loops = worker_loops
        # for creating an initial context dictionary when each worker loop starts
        self.worker_context_init_functions = worker_context_init_functions

        self.task_send_channel = None  # gets set by: execute_graph()

        self.nodes = {}
        self.edges_by_label = defaultdict(list)
        self.conditions = {}

        # list of any tasks created before execute_graph() is called
        # NOTE: once this is depleted by execute_graph(), it's no longer used
        self.unscheduled_tasks = []

    async def _schedule_new_task(self, task_node):
        # when a new task has no pre-conditions, or all pre-conditions are
        # already satisfied, schedule it immediately
        if self.task_send_channel:
            await self.task_send_channel.send(task_node.uid)
        else:
            # graph hasn't started 'executing' yet, store them here temporarily
            self.unscheduled_tasks.append(task_node.uid)

    async def create_task(self, task_handle, task_arguments, waits_on=None, attach_to_root_if_no_predecessors=True):

        status = 'created'
        if waits_on:
            status = 'waiting_for_predecessors'
        elif self.task_send_channel:
            status = 'ready_for_execution'

        task_node = TaskNode(self, task_handle, task_arguments, status)
        self.nodes[task_node.uid] = task_node

        if not waits_on:
            if attach_to_root_if_no_predecessors:
                # attach task_node to the graph, which acts as a 'root' node
                self.edges_by_label['scheduled'].append(task_node.uid)
            await self._schedule_new_task(task_node)
            return task_node

        waits_on = waits_on or []

        # waits_on should be a list of task_uids or condition_uids, never a mixture of both
        if all(uid.startswith('task-') for uid in waits_on):
            pre_conditions = [GenericSchedulingCondition(self, 'SUCCESS__ALL', waits_on)]
        elif all(uid.startswith('condition-') for uid in waits_on):
            pre_conditions = [self.conditions[uid] for uid in waits_on]
        else:
            exit('error: create_task() "waits_on" must be a list of task_uids or condition_uids')

        # add edges
        for condition in pre_conditions:
            condition.edges_by_label['waited_on_by'].append(task_node.uid)
            task_node.edges_by_label['waits_on'].append(condition.uid)

        if all(cond.condition_status == 'satisfied' for cond in pre_conditions):
            # schedule task if all pre-conditions are already satisfied
            await self._schedule_new_task(task_node)

        return task_node

    def draw(self):
        draw_graph__pyvis(self)

    def is_finished(self):

        # is this sufficient or do we need to traverse the graph for failed conditions
        # and exclude antecedents from the nodes we inspect?

        task_statuses = set([
            task.task_status for (uid, task) in self.nodes.items()
        ])

        if 'in_progress' in task_statuses or 'created' in task_statuses:
            return False

        condition_statuses = set([
            c.condition_status for (uid, c) in self.conditions.items()
        ])

        if condition_statuses.issubset({'satisfied', 'failed'}) and task_statuses.issubset(
                {'failed', 'precondition_failed', 'success'}):
            return True

        return False
