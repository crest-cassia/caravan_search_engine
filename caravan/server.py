import sys, logging, os, asyncio
from collections import defaultdict

from .task import Task
from .run import Run
from .parameter_set import ParameterSet


class Server(object):
    _observed_task = None
    _logger = None
    _out = None
    _max_submitted_task_id = 0
    _num_submitted = 0

    @classmethod
    def start(cls, coroutine, logger=None, redirect_stdout=False):
        cls._logger = logger or cls._default_logger()
        cls._observed_task = defaultdict(list)
        cls._out = os.fdopen(sys.stdout.fileno(), mode='w', buffering=1)
        sys.stdin = os.fdopen(sys.stdin.fileno(), mode='r', buffering=1)
        if redirect_stdout:
            sys.stdout = sys.stderr
        async def _main():
            t = asyncio.create_task(coroutine)
            await asyncio.gather(
                    t,
                    cls._loop(t)
                    )
        asyncio.run(_main())

    @classmethod
    async def task_completion(cls, task):
        if not task.is_finished():
            event = asyncio.Event()
            cls._observed_task[task.id].append(event)
            await event.wait()
        return task

    @classmethod
    async def ps_completion(cls, ps):
        coroutines = [cls.task_completion(r) for r in ps.runs() if not r.is_finished()]
        await asyncio.gather( *coroutines )
        return ps

    @classmethod
    async def _loop(cls, user_task):
        cls._logger.debug("start polling")
        while not user_task.done() or not cls._scheduler_ending():
            await asyncio.sleep(0)
            if cls._scheduler_ending(): # to prevent scheduler from ending
                cls._logger.debug("scheduler is ending, but user_task is not complete")
                cls._logger.debug(cls._observed_task)
                await asyncio.sleep(0.1)
                continue
            cls._submit_all()
            t = cls._receive_result()
            cls._num_submitted -= 1
            if t is None:
                break
            if t.id in cls._observed_task:
                events = cls._observed_task[t.id]
                for e in events:
                    e.set()
                del cls._observed_task[t.id]
        cls._finalize_scheduler()
        cls._logger.debug("event loop done")

    @classmethod
    def _default_logger(cls):
        logger = logging.getLogger(__name__)
        log_level = logging.INFO
        if 'CARAVAN_SEARCH_ENGINE_LOGLEVEL' in os.environ:
            s = os.environ['CARAVAN_SEARCH_ENGINE_LOGLEVEL']
            levels = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR,
                      'CRITICAL': logging.CRITICAL}
            log_level = levels[s]
        logger.setLevel(log_level)
        logger.propagate = False
        if not logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(log_level)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        return logger

    @classmethod
    def _scheduler_ending(cls):
        has_pending_tasks = (len(Task.all()) != cls._max_submitted_task_id)
        return not has_pending_tasks and cls._num_submitted == 0

    @classmethod
    def _submit_all(cls):
        tasks_to_be_submitted = [t for t in Task.all()[cls._max_submitted_task_id:] if not t.is_finished()]
        n = len(tasks_to_be_submitted)
        cls._logger.debug("submitting %d Tasks" % n)
        cls._num_submitted += n
        cls._print_tasks(tasks_to_be_submitted)
        cls._max_submitted_task_id = len(Task.all())

    @classmethod
    def _print_tasks(cls, tasks):
        for t in tasks:
            line = "%d %s\n" % (t.id, t.command)
            cls._out.write(line)
        cls._out.write("\n")

    @classmethod
    def _finalize_scheduler(cls):
        cls._print_tasks([])

    @classmethod
    def _receive_result(cls):
        line = sys.stdin.readline()
        if not line: return None
        line = line.rstrip()
        cls._logger.debug("received: %s" % line)
        if not line: return None
        l = line.split(' ')
        tid, rc, place_id, start_at, finish_at = [int(x) for x in l[:5]]
        results = [float(x) for x in l[5:]]
        t = Task.find(tid)
        t.store_result(results, rc, place_id, start_at, finish_at)
        cls._logger.debug("stored result of Task %d" % tid)
        return t

    @classmethod
    def _debug(cls):
        print(str(cls._observed_task), file=sys.stderr, flush=True)
