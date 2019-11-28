import traceback


def get_exception_traceback(ex):
    tb_lines = traceback.format_exception(
        ex.__class__, ex, ex.__traceback__
    )
    return '\n'.join(tb_lines)

