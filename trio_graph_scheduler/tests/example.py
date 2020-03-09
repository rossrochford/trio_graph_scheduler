"""

Task scheduling example to fetch html content, extract word counts per page,
and then combine word counts from each task.

For graph illustration see: tests/example_graph.png
"""

from collections import defaultdict
import re

import asks
import trio

from trio_graph_scheduler.execution import execute_graph
from trio_graph_scheduler.graph import TaskGraph


HTML_CONTENT_REGEX = r'>([^<>]{3,})<'

STOPWORDS = (
    'it', 'the', 'and', 'in', 'by', 'to', 'a', 'of', 'for',
    'is', 'be', 'with', 'on', 'that', 'are', 'or', 'an', 'as'
)

WORKER_LOOP_ASSIGNMENTS = {
    # assign get_page_source() to 'html_worker_loop' to limit concurrency
    'get_page_source': 'html_worker_loop'
}

WORKER_LOOPS = {
    # { name: number of concurrent workers }
    'default': 4,
    # limit the number of concurrent tasks that use HTTP
    'html_worker_loop': 10
}


async def combine_page_word_counts(word_counts_by_url, **kwargs):

    word_counts = defaultdict(int)

    for url, page_word_counts in word_counts_by_url.items():
        for word in page_word_counts.keys():
            word_counts[word] += page_word_counts[word]

    word_counts_list = [tup for tup in word_counts.items()]
    word_counts_list.sort(key=lambda tup: tup[1], reverse=True)

    top3 = [tup[0] for tup in word_counts_list[:8]]
    print('top 3 words: %s' % top3)

    return word_counts


async def get_word_counts(predecessors, **kwargs):
    """
    Args:
        predecessors (list):  list (length 1) of previous get_page_source() task
    """
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
        import pdb; pdb.set_trace()
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

    async def get_args__combine_page_word_counts(predecessors, **kwargs):
        # sometimes we might pass a function to calculate arguments when predecessors are complete
        word_counts_by_url = {
            # url:  word_counts
            node.task_result[0]: node.task_result[1] for node in predecessors
        }
        # return value should be tuple in the form:  ((arg1, arg2, ), {})
        return (word_counts_by_url,), {}

    graph = TaskGraph(FUNCTIONS, WORKER_LOOPS, WORKER_LOOP_ASSIGNMENTS)

    page_source__task_uids = []
    word_count__task_uids = []

    for url in page_urls:
        uid = await graph.create_task(
            'get_page_source', ((url,), {}), graph.root.uid
        )
        page_source__task_uids.append(uid)

        uid = await graph.create_wait_task(
            # when args is set to None, they'll be replaced with a list of predecessors
            'get_word_counts', None, [uid], 'SUCCESS_ALL'
        )
        word_count__task_uids.append(uid)

    # when all get_word_counts() tasks are complete, combine results
    await graph.create_wait_task(
        'combine_page_word_counts',
        get_args__combine_page_word_counts,  # - when args is a function, it's called before task executes to get args
        word_count__task_uids,               # - wait for all page word counts to complete
        'COMPLETE_ALL'                       # - schedule this task regardless of whether its predecessor tasks succeed or fail
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


if __name__ == '__main__':
    trio.run(main)
