from collections import defaultdict, namedtuple
import re

import asks
import trio

from trio_graph_scheduler.execution import execute_graph
from trio_graph_scheduler.scheduling import GenericSchedulingCondition
from trio_graph_scheduler.graph import TaskGraph


HTML_CONTENT_REGEX = r'>([^<>]{3,})<'

STOPWORDS = (
    'it', 'the', 'and', 'in', 'by', 'to', 'a', 'of', 'for',
    'is', 'be', 'with', 'on', 'that', 'are', 'or', 'an', 'as'
)


async def combine_page_word_counts(**kwargs):

    predecessors, _ = kwargs['task_node'].get_predecessor_task_nodes()

    word_counts_by_url = {
        # url:  word_counts
        node.task_result[0]: node.task_result[1] for node in predecessors
        if node.task_status == 'success'
    }
    word_counts = defaultdict(int)

    for url, page_word_counts in word_counts_by_url.items():
        for word in page_word_counts.keys():
            word_counts[word] += page_word_counts[word]

    word_counts_list = [tup for tup in word_counts.items()]
    word_counts_list.sort(key=lambda tup: tup[1], reverse=True)

    top3 = [tup[0] for tup in word_counts_list[:13]]
    print('top 13 words: %s' % top3)
    return word_counts


async def get_word_counts(**kwargs):

    predecessors, _ = kwargs['task_node'].get_predecessor_task_nodes()

    word_counts = defaultdict(int)

    url = predecessors[0].task_arguments[0][0]  # a little hacky
    page_source = predecessors[0].task_result

    for content in re.findall(HTML_CONTENT_REGEX, page_source, flags=re.M):
        for word in content.split(' '):
            word = word.strip().lower().rstrip(',.:)').lstrip('(').strip()
            if not word:
                continue
            if word in STOPWORDS:
                continue
            word_counts[word] += 1

    return url, word_counts


async def get_page_source(url, **kwargs):

    resp = await asks.get(url)
    if resp.status_code != 200:
        # will cause TaskNode.status to be set to 'failed'
        raise Exception('http get failed with status: %s' % resp.status_code)
        # or you could set 'failed' status here and return

    return resp.content.decode()


async def create_graph__fetch_word_counts(page_urls):

    FUNCTIONS = {
        'get_page_source': get_page_source,
        'get_word_counts': get_word_counts,
        'combine_page_word_counts': combine_page_word_counts,
    }
    WORKER_LOOPS = [
        TaskGraph.WorkerLoop('default', 3, None),
        TaskGraph.WorkerLoop('html_loop', 4, ['get_page_source'])
    ]

    graph = TaskGraph(FUNCTIONS, WORKER_LOOPS)

    word_count__task_uids = []

    for url in page_urls:
        ps_task = await graph.create_task(
            'get_page_source', ((url,), {}), None
        )
        wc_task = await graph.create_task(
            'get_word_counts', None, [ps_task.uid]
        )
        word_count__task_uids.append(wc_task.uid)

    wait_condition = GenericSchedulingCondition(
        graph, 'COMPLETE__ALL', word_count__task_uids
    )
    await graph.create_task(
        'combine_page_word_counts', None, [wait_condition.uid]
    )
    return graph


async def main():

    URLS = [
        'https://trio.readthedocs.io/en/stable/',
        'https://asks.readthedocs.io/en/latest/index.html',
        'https://docs.python.org/3/whatsnew/3.8.html',
    ]

    graph = await create_graph__fetch_word_counts(URLS)

    await execute_graph(graph, 3)

    await trio.sleep(3)

    graph.draw()


if __name__ == '__main__':
    trio.run(main)


