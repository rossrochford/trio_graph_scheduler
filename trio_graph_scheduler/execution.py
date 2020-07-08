
import trio

from trio_graph_scheduler.util import get_exception_traceback


async def create_worker_context(graph, worker_type, worker_index):
    if graph.worker_context_init_functions is None:
        return {}
    if worker_type not in graph.worker_context_init_functions:
        return {}
    init_func = graph.worker_context_init_functions[worker_type]
    return await init_func(worker_index)


async def worker_loop(graph, worker_type, worker_index, task_receive_channel):
    """ Repeatedly get a task_node_uid and execute it. """

    worker_context = await create_worker_context(
        graph, worker_type, worker_index
    )

    async with task_receive_channel:
        async for task_node_uid in task_receive_channel:
            task_node = graph.nodes[task_node_uid]
            await execute_task(task_node, worker_context)


async def task_loop_dispatcher(
    graph, task_receive_channel, worker_send_channels
):

    worker_loops_by_task_handle = {}
    for wl_tuple in graph.worker_loops:
        if wl_tuple.name == 'default':
            continue
        for task_handle in wl_tuple.functions:
            worker_loops_by_task_handle[task_handle] = wl_tuple.name

    # Dispatch task uids to their appropriate worker loop
    async with task_receive_channel:
        async for task_node_uid in task_receive_channel:
            print('dispatching %s' % task_node_uid)
            task = graph.nodes[task_node_uid]

            worker_loop_type = worker_loops_by_task_handle.get(task.task_handle, 'default')

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

        await trio.sleep(4)


async def execute_task(task_node, worker_context):

    self = task_node
    self.task_status = 'in_progress'
    # await self.log('INFO', 'starting task')

    try:
        if self.task_arguments is None:
            args, kwargs = [], {}
        else:
            args, kwargs = self.task_arguments
        args = args or []
        kwargs = kwargs or {}
        kwargs['worker_context'] = worker_context
        kwargs.update({'task_node': self, 'graph': self.graph})

        function = self.graph.expected_functions[self.task_handle]
        self.task_result = await function(*args, **kwargs)

    except Exception as e:
        self.exception_trace = get_exception_traceback(e)
        self.task_status = 'failed'
        print('%s: %s' % (self.task_handle, self.task_status))
        print('exception thrown: ' + self.exception_trace)
        for uid in self.edges_by_label['waited_on_by']:
            await self.graph.conditions[uid].notify_predecessor_task_complete(self)
        return

    self.task_completed_event.set()
    self.task_status = 'success'
    print('%s: %s' % (self.task_handle, self.task_status))

    for uid in self.edges_by_label['waited_on_by']:
        await self.graph.conditions[uid].notify_predecessor_task_complete(self)


async def execute_graph(graph, finish_polling_initial_delay=30):

    task_send_channel, task_receive_channel = trio.open_memory_channel(20)
    finish_send_channel, finish_receive_channel = trio.open_memory_channel(0)

    async with task_receive_channel, task_send_channel, finish_receive_channel, finish_send_channel:

        # indicates to graph.create_task() that graph execution has begun
        graph.task_send_channel = task_send_channel

        async with trio.open_nursery() as nursery:

            dispatcher_send_channels = {}

            for wl_tuple in graph.worker_loops:

                send_channel, receive_channel = trio.open_memory_channel(20)
                dispatcher_send_channels[wl_tuple.name] = send_channel

                for idx in range(wl_tuple.concurrency):
                    recv_channel = receive_channel if idx == 0 else receive_channel.clone()
                    nursery.start_soon(
                        worker_loop, graph, wl_tuple.name, idx, recv_channel
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
                graph.nodes[task_uid].task_status = 'ready_for_execution'
                await graph.task_send_channel.send(task_uid)

            async for signal in finish_receive_channel:
                print('closing nursery')
                nursery.cancel_scope.cancel()
