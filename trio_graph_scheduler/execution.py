import trio

from trio_graph_scheduler.nodes import (
    TaskNode, WaitTaskNode, TaskGroupThresholdNode, SubTask, NodeQuerySet
)
from trio_graph_scheduler.util import get_exception_traceback

FUNCTION_TYPE = type(lambda: 1)


async def create_worker_context(graph, worker_type, worker_index):
    if graph.WORKER_CONTEXT_INIT_FUNCTIONS is None:
        return {}
    if worker_type not in graph.WORKER_CONTEXT_INIT_FUNCTIONS:
        return {}
    init_func = graph.WORKER_CONTEXT_INIT_FUNCTIONS[worker_type]
    return await init_func(worker_index)


async def worker_loop(graph, worker_type, worker_index, task_receive_channel):
    """ Repeatedly get a task_node_uid and execute it. """

    worker_context = await create_worker_context(
        graph, worker_type, worker_index
    )

    async with task_receive_channel:
        async for task_node_uid in task_receive_channel:
            task_node = graph.nodes[task_node_uid]

            if type(task_node) in (TaskNode, SubTask):
                # NOTE: isinstance() might be better, it would allow for sub-classing
                await execute_task(task_node, worker_context)

            elif type(task_node) is WaitTaskNode:
                await execute_wait_task(task_node, worker_context)

            elif isinstance(task_node, TaskGroupThresholdNode):
                # NOTE: group tasks don't need worker_context
                await execute_task_group_threshold_node(task_node)


async def task_loop_dispatcher(
    graph, task_receive_channel, worker_send_channels
):
    """ Dispatch task uids to their appropriate worker loop"""
    async with task_receive_channel:
        async for task_node_uid in task_receive_channel:

            task = graph.nodes[task_node_uid]
            if isinstance(task, TaskGroupThresholdNode):
                worker_loop_type = 'default'
            else:
                worker_loop_type = graph.WORKER_LOOP_ASSIGNMENTS.get(
                    task.task_handle, 'default'
                )

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

    if self.depth is None:
        if self.prev and self.prev.depth is not None:
            self.depth = self.prev.depth + 1

    assert type(self.task_arguments) is tuple and len(self.task_arguments) == 2

    try:
        args, kwargs = self.task_arguments
        args = args or []
        kwargs = kwargs or {}
        kwargs['worker_context'] = worker_context
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
    self.completion_event.set()

    for uid in self.edges_by_label['waited_on_by']:
        node = self.graph.nodes[uid]
        await node.notify_predecessor_complete()


async def execute_wait_task(wait_task, worker_context):

    self = wait_task

    predecessor_uids = self.edges_by_label['waits_on']
    predecessors = NodeQuerySet(
        [self.graph.nodes[uid] for uid in predecessor_uids]
    )

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

    await execute_task(self, worker_context)


async def _grouptask_completion_cleanup(graph, group_task, sub_tasks, status):
    # all sub-tasks completed successfully, so:
    # notify any wait-tasks, set status, set completion_event

    # note: status needs to be set before notifying dependent wait-tasks
    group_task.task_status = status
    group_task.completion_event.set()

    # cancel any sub-tasks that haven't yet started executing, along with their wait-tasks
    for n in sub_tasks:
        if n.task_status in ('waiting_for_parent_TaskGroupThresholdNode', 'ready_for_execution'):
            n.task_status = 'cancelled'
            for uid in n.edges_by_label['waited_on_by']:
                wait_task_of_subtask = graph.nodes[uid]
                wait_task_of_subtask.task_status = 'cancelled'
                # NOTE: we're not doing cancelling recursively here

    for uid in group_task.edges_by_label['waited_on_by']:
        node = graph.nodes[uid]
        await node.notify_predecessor_complete()


async def execute_task_group_threshold_node(group_task):

    graph = group_task.graph
    all_subtasks = group_task.subtasks
    target_num = group_task.target_num_successful

    assert len(all_subtasks) >= target_num

    for subtask_node in all_subtasks[:target_num]:
        subtask_node.task_status = 'ready_for_execution'
        print('sending task for execution: %s' % subtask_node.uid[:6])
        await graph.task_send_channel.send(subtask_node.uid)

    pending_subtasks = all_subtasks[:target_num]
    remainder_pos = target_num
    failed_subtask_found = False

    while pending_subtasks:

        subtask_node = pending_subtasks.pop(0)
        await subtask_node.wait_for_completion(log=True)

        if subtask_node.task_status != 'success':
            failed_subtask_found = True
            if subtask_node.task_status != 'failed':
                import pdb; pdb.set_trace()
            if remainder_pos == len(all_subtasks):
                break

            subtask_node.task_status = 'ready_for_execution'
            await graph.task_send_channel.send(subtask_node.uid)

            pending_subtasks.append(all_subtasks[remainder_pos])
            remainder_pos += 1

    if failed_subtask_found:
        import pdb; pdb.set_trace()  # todo: inspect

    if group_task.is_successful():
        await _grouptask_completion_cleanup(
            graph, group_task, all_subtasks, 'success'
        )
    else:
        import pdb; pdb.set_trace()
        await _grouptask_completion_cleanup(
            graph, group_task, all_subtasks, 'failed'
        )

    '''
    for i, subtask_node in enumerate(all_subtasks[:target_num], 1):
        print('GroupTask: waiting for initial subtask %s/%s' % (i, target_num))
        await subtask_node.completion_event.wait()
        print('GroupTask: subtask done: %s/%s' % (i, target_num))
        if subtask_node.task_status != 'success':
            assert subtask_node.task_status == 'failed'

    
    successful_tasks = [n for n in all_subtasks if n.task_status == 'success']
    if len(successful_tasks) == target_num:
        await _grouptask_completion_cleanup(
            graph, group_task, all_subtasks, 'success'
        )
        return

    iter_count = 0
    while len(successful_tasks) < target_num:
        remainder = [n for n in all_subtasks if n.task_status == 'waiting_for_parent_TaskGroupThresholdNode']
        remainder = remainder[:target_num-len(successful_tasks)]
        if len(remainder) == 0:
            break
        for subtask_node in remainder:
            subtask_node.task_status = 'ready_for_execution'
            await graph.task_send_channel.send(subtask_node.uid)
        for i, subtask_node in enumerate(remainder, 1):
            print('GroupTask: waiting for initial subtask %s/%s' % (i, len(remainder)))
            await subtask_node.completion_event.wait()
            print('GroupTask: subtask done: %s/%s' % (i, len(remainder)))
            if subtask_node.task_status == 'success':
                successful_tasks.append(subtask_node)
        iter_count += 1
        if iter_count > 4:
            break
        print('GroupTask: retrying, successful: %s, target: %s' %
            (len(successful_tasks), target_num)
        )

    if len(successful_tasks) == target_num:
        await _grouptask_completion_cleanup(
            group_task, group_task, all_subtasks, 'success'
        )
    else:
        import pdb; pdb.set_trace()
        await _grouptask_completion_cleanup(
            group_task, group_task, all_subtasks, 'failed'
        )
    '''


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

                for idx in range(num_workers):
                    recv_channel = receive_channel if idx == 0 else receive_channel.clone()
                    nursery.start_soon(
                        worker_loop, graph, worker_loop_type, idx, recv_channel
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
                print('closing nusery')
                nursery.cancel_scope.cancel()
