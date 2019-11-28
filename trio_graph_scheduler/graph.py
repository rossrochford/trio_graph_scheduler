import uuid

from trio_graph_scheduler.nodes import Node, TaskNode, WaitTaskNode


class TaskGraph(object):

    def __init__(self, expected_functions, worker_loops, work_loop_assignments=None):
        self.uid = uuid.uuid4().hex
        self.nodes = {}
        self.root = Node(self)
        self.expected_functions = expected_functions

        assert 'default' in worker_loops
        self.WORKER_LOOPS = worker_loops
        self.WORKER_LOOP_ASSIGNMENTS = work_loop_assignments or {}

        self.task_send_channel = None  # gets set by: execute_graph()

        # list of any tasks that were created before execute_graph() was called
        # NOTE: this is never again used once it has been depleted by execute_graph()
        self.unscheduled_tasks = []

    async def create_task(self, task_handle, task_arguments, predecessor_uid):

        status = 'ready_for_execution' if self.task_send_channel else 'created'
        task_node = TaskNode(self, task_handle, task_arguments, status)

        # (predecessor) ---[scheduled]---> (task_node)
        self.nodes[predecessor_uid].add_edge(task_node, 'scheduled')

        if self.task_send_channel:
            await self.task_send_channel.send(task_node.uid)
        else:
            self.unscheduled_tasks.append(task_node.uid)

        return task_node.uid

    async def create_wait_task(
            self, function_name, arguments,
            predecessor_uids, schedule_when_errors_found
    ):
        assert len(predecessor_uids) > 0

        task_node = WaitTaskNode(
            self, function_name, arguments, schedule_when_errors_found
        )

        for uid in predecessor_uids:
            predecessor = self.nodes[uid]
            # edges in both directions
            predecessor.add_edge(task_node, 'waited_on_by')  # (predecessor) -- [waited_on_by] -->  (task_node)
            task_node.add_edge(predecessor, 'waits_on')      # (predecessor) <---- [waits_on] ----  (task_node)

        # in case predecessors have already completed
        if self.task_send_channel:
            async with task_node.notify_lock:
                if task_node.is_ready():
                    task_node.task_status = 'ready_for_execution'
                    await self.task_send_channel.send(task_node.uid)

        return task_node.uid

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
            task for (uid, task) in self.nodes.items() if task != self.root
        ]
        statuses = set([n.task_status for n in task_nodes])
        if statuses == {'success'} or statuses == {'success', 'failed'}:
            return True
        return False
