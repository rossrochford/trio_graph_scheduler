import trio

from trio_graph_scheduler.nodes import TaskNode, WaitTaskNode
from trio_graph_scheduler.util import get_exception_traceback

FUNCTION_TYPE = type(lambda: 1)


async def worker_loop(graph, task_receive_channel):
    """ Repeatedly get a task_node_uid and execute it. """

    async with task_receive_channel:
        async for task_node_uid in task_receive_channel:
            task_node = graph.nodes[task_node_uid]

            if type(task_node) is TaskNode:
                await execute_task(task_node)

            elif type(task_node) is WaitTaskNode:
                await execute_wait_task(task_node)


async def task_loop_dispatcher(
        graph, task_receive_channel, worker_send_channels
):
    """ Dispatch task uids to their appropriate worker loop"""
    async with task_receive_channel:
        async for task_node_uid in task_receive_channel:

            task_handle = graph.nodes[task_node_uid].task_handle
            worker_loop_type = graph.WORKER_LOOP_ASSIGNMENTS.get(task_handle, 'default')

            await worker_send_channels[worker_loop_type].send(task_node_uid)


async def poll_until_finished(graph, finish_send_channel, initial_delay):

    await trio.sleep(initial_delay)

    while True:
        print('polling graph.is_finished()')
        if graph.is_finished():
            # just in case, wait 1 second and try again
            await trio.sleep(1)

            if graph.is_finished():
                print('closing...')
                await finish_send_channel.send('x')
                break

        await trio.sleep(3)


async def execute_task(task_node):

    self = task_node
    self.task_status = 'in_progress'
    # await self.log('INFO', 'starting task')

    assert type(self.task_arguments) is tuple and len(self.task_arguments) == 2

    try:
        args, kwargs = self.task_arguments
        args = args or []
        kwargs = kwargs or {}
        kwargs.update(self._get_extra_kwargs())

        function = self.graph.expected_functions[self.task_handle]
        self.task_result = await function(*args, **kwargs)

        if self.task_status != 'failed':  # in case the task set status to 'failed'
            self.task_status = 'success'

    except Exception as e:
        self.exception_trace = get_exception_traceback(e)
        self.task_status = 'failed'

        print('exception thrown: ' + self.exception_trace)
        return

    # await self.log('INFO', 'task complete')

    for uid in self.edges_by_label['waited_on_by']:
        node = self.graph.nodes[uid]
        await node.notify_predecessor_complete()


async def execute_wait_task(wait_task):

    self = wait_task

    predecessor_uids = self.edges_by_label['waits_on']
    predecessors = [self.graph.nodes[uid] for uid in predecessor_uids]

    if type(self.task_arguments) is FUNCTION_TYPE:
        try:
            self.task_arguments = await self.task_arguments(predecessors)
        except Exception as e:
            # usually this happens when task_arguments() decides the
            # task can't be executed based on predecessor task results
            self.exception_trace = get_exception_traceback(e)
            self.task_status = 'failed'

            print('exception thrown: ' + self.exception_trace)
            return
    elif self.task_arguments is None:
        self.task_arguments = ((predecessors, ), {})

    await execute_task(self)


async def execute_graph(graph, finish_polling_initial_delay=60):

    task_send_channel, task_receive_channel = trio.open_memory_channel(20)
    finish_send_channel, finish_receive_channel = trio.open_memory_channel(0)

    async with task_receive_channel, task_send_channel, finish_receive_channel, finish_send_channel:

        graph.task_send_channel = task_send_channel

        async with trio.open_nursery() as nursery:

            dispatcher_send_channels = {}
            for worker_loop_type, num_workers in graph.WORKER_LOOPS.items():

                send_channel, receive_channel = trio.open_memory_channel(20)
                dispatcher_send_channels[worker_loop_type] = send_channel

                for i in range(num_workers):
                    recv_channel = receive_channel if i == 0 else receive_channel.clone()
                    nursery.start_soon(
                        worker_loop, graph, recv_channel
                    )

            nursery.start_soon(
                task_loop_dispatcher, graph,
                task_receive_channel, dispatcher_send_channels
            )

            nursery.start_soon(
                poll_until_finished, graph,
                finish_send_channel, finish_polling_initial_delay
            )

            while graph.unscheduled_tasks:
                task_uid = graph.unscheduled_tasks.pop(0)
                graph.nodes[task_uid].status = 'ready_for_executed'
                await graph.task_send_channel.send(task_uid)

            async for signal in finish_receive_channel:
                print('closing nusery')
                nursery.cancel_scope.cancel()
