from collections import defaultdict
import uuid

import trio


IN_PROGRESS_STATUSES = (
    'ready_for_execution', 'in_progress', 'waiting_for_predecessors'
)


class SchedulingCondition(object):
    pass


class GenericSchedulingCondition(SchedulingCondition):

    def __init__(self, graph, schedule_mode, waits_on=None):

        self.graph = graph
        self.uid = 'condition-' + uuid.uuid4().hex

        self.notify_lock = trio.Lock()
        self.condition_satisfied_event = trio.Event()

        self.condition_status = 'waiting_on_tasks'
        self.edges_by_label = defaultdict(list)

        for task_uid in (waits_on or []):
            task = self.graph.nodes[task_uid]
            task.edges_by_label['waited_on_by'].append(self.uid)
            self.edges_by_label['waits_on'].append(task_uid)

        self.completion_criteria, self.target_num = schedule_mode.split('__')
        assert self.completion_criteria in ('SUCCESS', 'COMPLETE')

        if self.target_num.isdigit():
            self.target_num = int(self.target_num)
        else:
            assert self.target_num == 'ALL'

        self.graph.conditions[self.uid] = self

    @property
    def predecessor_statuses(self):
        nodes = self.graph.nodes
        return [nodes[uid].task_status for uid in self.edges_by_label['waits_on']]

    def is_ready(self):
        if self.condition_status == 'satisfied':
            # note: this implies that once a condition is satisfied, no new predecessor
            # tasks should be added to it, should we enforce this?
            return True

        predecessor_statuses = self.predecessor_statuses

        if self.completion_criteria == 'COMPLETE':
            if self.target_num == 'ALL':
                for status in predecessor_statuses:
                    if status in IN_PROGRESS_STATUSES:
                        return False
                return True
            else:
                num_complete = predecessor_statuses.count('success') + predecessor_statuses.count('failed')
                return num_complete >= self.target_num

        # completion_criteria == 'SUCCESS'
        num_successful = predecessor_statuses.count('success')
        threshold = len(predecessor_statuses) if self.target_num == 'ALL' else self.target_num
        if num_successful >= threshold:
            return True
        return False

    async def handle_failed_predecessor(self):
        # only gets called when completion_criteria == 'SUCCESS'

        if self.target_num == 'ALL':
            self.condition_status = 'failed'
            for uid in self.edges_by_label['waited_on_by']:
                await self.graph.nodes[uid].notify_predecessor_condition_failed()
            return

        num_predecessors = len(self.edges_by_label['waits_on'])
        threshold = num_predecessors if self.target_num == 'ALL' else self.target_num

        failure_threshold = num_predecessors - threshold
        predecessor_statuses = self.predecessor_statuses
        num_failed = predecessor_statuses.count('failed') + predecessor_statuses.count('precondition_failed')

        if num_failed >= failure_threshold:
            self.condition_status = 'failed'
            for uid in self.edges_by_label['waited_on_by']:
                await self.graph.nodes[uid].notify_predecessor_condition_failed()

    async def notify_predecessor_task_complete(self, task_node):

        if self.condition_status == 'failed':
            return

        async with self.notify_lock:
            if task_node.task_status == 'failed' and self.completion_criteria == 'SUCCESS':
                await self.handle_failed_predecessor()
                if self.condition_status == 'failed':
                    return

            if self.is_ready():
                self.condition_satisfied_event.set()
                self.condition_status = 'satisfied'
                # schedule dependent tasks
                for uid in self.edges_by_label['waited_on_by']:
                    await self.graph.nodes[uid].notify_predecessor_condition_satisfied()
