import traceback


def get_exception_traceback(ex):
    tb_lines = traceback.format_exception(
        ex.__class__, ex, ex.__traceback__
    )
    return '\n'.join(tb_lines)


EDGE_COLOURS = {
    'waits_on': 'blue',
    'waited_on_by': 'red',
    'scheduled': 'purple'
}

TASK_STATUS_COLOURS = {
    'created': 'orange',
    'ready_for_execution': 'orange',
    'failed': 'red',
    'precondition_failed': 'red',
    'success': 'green'
}

CONDITION_STATUS_COLOURS = {
    'waiting_on_tasks': 'orange',
    'satisfied': 'green',
    'failed': 'red'
}

# created
# ready_for_execution
# failed
# success

# waiting_on_tasks
# satisfied


def draw_graph__pyvis(graph):

    # to use, install pyvis:  pip install pyvis

    def _get_node_appearance(item):
        uid = item.uid
        label = uid
        color = '#97c2fc'
        shape = 'circle'
        if uid.startswith('condition-'):
            label = 'C-' + uid.split('-')[1][:6]
            color = CONDITION_STATUS_COLOURS[item.condition_status]
            shape = 'dot'
        elif uid.startswith('task-'):
            label = 'T-' + items[uid].task_handle[:15]
            color = TASK_STATUS_COLOURS[item.task_status]
            shape = 'box'
        elif uid.startswith('graph-'):
            label = 'root'
            color = 'yellow'
            shape = 'circle'
        return label, color, shape

    def _draw_branch(network, items, node, edges_visited):
        for label, item_uids in node.edges_by_label.items():
            for uid in item_uids:
                if (node.uid, uid, label) in edges_visited:
                    continue
                edges_visited.append(
                    (node.uid, uid, label)
                )

                network.add_edge(node.uid, uid, title=label, color=EDGE_COLOURS[label])
                _draw_branch(network, items, items[uid], edges_visited)

    from pyvis.network import Network

    got_net = Network(height="1400px", width="100%", bgcolor="#222222", font_color="white", directed=True)
    got_net.set_edge_smooth('horizontal')  # 'straightCross'

    # collapse conditions and nodes into a single dictionary
    items = graph.nodes.copy()
    items.update(graph.conditions)
    items[graph.uid] = graph

    for uid, item in items.items():
        size = 12 if uid.startswith('condition-') else 24

        label, color, shape = _get_node_appearance(item)

        got_net.add_node(uid, label=label, size=size, color=color, shape=shape)

    edges_visited = []
    _draw_branch(got_net, items, graph, edges_visited)

    # set the physics layout of the network
    got_net.hrepulsion()  # barnes_hut()

    got_net.show("graph.html")


'''
def draw_graph__pygraphviz(graph):

    def _draw_branch(a_graph, items, node, edges_visited):
        for label, item_uids in node.edges_by_label.items():
            for uid in item_uids:
                if (node.uid, uid, label) in edges_visited:
                    continue
                edges_visited.append(
                    (node.uid, uid, label)
                )
                a_graph.add_edge(node.uid[:13], uid[:13])
                _draw_branch(a_graph, items, items[uid], edges_visited)

    items = graph.nodes.copy()
    items.update(graph.conditions)
    items[graph.uid] = graph

    import pygraphviz as pgv
    A = pgv.AGraph(strict=False, directed=True)

    edges_visited = []
    _draw_branch(A, items, graph, edges_visited)

    A.draw('schedule.png', prog='fdp')
    print('wrote schedule.png')
'''
