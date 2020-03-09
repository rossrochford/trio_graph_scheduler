import uuid

from trio_graph_scheduler.nodes import Node, TaskNode, WaitTaskNode, TaskGroupThresholdNode


class TaskGraph(object):

    def __init__(
        self, expected_functions, worker_loops,
        work_loop_assignments, worker_context_init_functions
    ):
        self.uid = uuid.uuid4().hex
        self.nodes = {}
        self.root = Node(self)
        self.root.depth = 0

        self.expected_functions = expected_functions

        assert 'default' in worker_loops
        self.WORKER_LOOPS = worker_loops
        self.WORKER_LOOP_ASSIGNMENTS = work_loop_assignments or {}
        self.WORKER_CONTEXT_INIT_FUNCTIONS = worker_context_init_functions
        self.task_send_channel = None  # gets set by: execute_graph()

        # list of any tasks that were created before execute_graph() was called
        # NOTE: this is never again used once it has been depleted by execute_graph()
        self.unscheduled_tasks = []

    async def create_task(self, task_handle, task_arguments, predecessor_uid):

        status = 'ready_for_execution' if self.task_send_channel else 'created'
        task_node = TaskNode(self, task_handle, task_arguments, status)

        # (predecessor) ---[scheduled]---> (task_node)
        self.nodes[predecessor_uid].add_edge(task_node, 'scheduled')
        task_node.add_edge(self.nodes[predecessor_uid], 'scheduled_by')

        if self.nodes[predecessor_uid].depth is not None:
            task_node.depth = self.nodes[predecessor_uid].depth + 1

        if self.task_send_channel:
            # task_send_channel is set when graph is executed
            await self.task_send_channel.send(task_node.uid)
        else:
            # this means that graph hasn't started 'executing' yet
            self.unscheduled_tasks.append(task_node.uid)

        return task_node.uid, task_node

    async def create_group_task(
        self, target_num_successful, predecessor_uid,
        cls=None, min_num_successful=None
    ):

        status = 'ready_for_execution' if self.task_send_channel else 'created'
        cls = cls or TaskGroupThresholdNode
        task_node = cls(
            self, target_num_successful,
            status=status, min_num_successful=min_num_successful
        )

        # (predecessor) ---[scheduled]---> (task_node)
        self.nodes[predecessor_uid].add_edge(task_node, 'scheduled')
        task_node.add_edge(self.nodes[predecessor_uid], 'scheduled_by')

        if self.nodes[predecessor_uid].depth is not None:
            task_node.depth = self.nodes[predecessor_uid].depth + 1

        if self.task_send_channel:
            # task_send_channel is set when graph is executed
            await self.task_send_channel.send(task_node.uid)
        else:
            # this means that graph hasn't started 'executing' yet
            self.unscheduled_tasks.append(task_node.uid)

        return task_node.uid, task_node

    async def create_wait_task(
        self, function_name, arguments,
        predecessor_uids, schedule_mode
    ):
        assert len(predecessor_uids) > 0

        task_node = WaitTaskNode(
            self, function_name, arguments, schedule_mode
        )

        predecessor_depths = []
        for uid in predecessor_uids:
            predecessor = self.nodes[uid]
            # edges in both directions
            predecessor.add_edge(task_node, 'waited_on_by')  # (predecessor) -- [waited_on_by] -->  (task_node)
            task_node.add_edge(predecessor, 'waits_on')      # (predecessor) <---- [waits_on] ----  (task_node)
            if predecessor.depth is not None:
                predecessor_depths.append(predecessor.depth)

        if predecessor_depths:
            task_node.depth = max(predecessor_depths) + 1

        # in case predecessors have already completed
        if self.task_send_channel:
            async with task_node.notify_lock:
                if task_node.is_ready():
                    task_node.task_status = 'ready_for_execution'
                    await self.task_send_channel.send(task_node.uid)

        return task_node.uid, task_node

    def create_wait_link(self, predecessor_uid, wait_task_uid):

        wait_node = self.nodes[wait_task_uid]
        assert type(wait_node) is WaitTaskNode

        predecessor_node = self.nodes[predecessor_uid]

        predecessor_node.add_edge(wait_node, 'waited_on_by')
        wait_node.add_edge(predecessor_node, 'waits_on')

    def get_task_nodes(self, status, handle):
        task_nodes = [
            task for (uid, task) in self.nodes.items() if task != self.root
        ]
        if status:
            task_nodes = [n for n in task_nodes if n.task_status == status]
        if handle:
            task_nodes = [n for n in task_nodes if n.task_handle == handle]
        return task_nodes

    def is_finished(self):
        task_nodes = [
            task for (uid, task) in self.nodes.items()
            if task != self.root and task.task_status != 'cancelled'
        ]
        statuses = set([n.task_status for n in task_nodes])
        if statuses == {'success'} or statuses == {'success', 'failed'}:
            return True
        # print('is_finished(): %s' % statuses)
        return False
