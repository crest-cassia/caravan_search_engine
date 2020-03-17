import sys, logging, os, msgpack, struct
from collections import defaultdict

if os.getenv("CARAVAN_USE_PSEUDO_FIBER") == "1":  # for debugging pseudo_fiber
    from .pseudo_fiber import Fiber
else:
    try:
        from fibers import Fiber
    except ImportError:
        from .pseudo_fiber import Fiber
from .task import Task
from .run import Run
from .parameter_set import ParameterSet


class Server(object):
    _instance = None

    @classmethod
    def get(cls):
        if cls._instance is None:
            raise Exception("use Server.start() method")
        return cls._instance

    def __init__(self, logger=None):
        self.observed_ps = defaultdict(list)
        self.observed_all_ps = defaultdict(list)
        self.observed_task = defaultdict(list)
        self.observed_all_tasks = defaultdict(list)  # (task_id) => list of (task_ids, callback)
        self.max_submitted_task_id = 0
        self._logger = logger or self._default_logger()
        self._fibers = []
        self._out = None
        self._in = None

    @classmethod
    def start(cls, logger=None, redirect_stdout=False):
        cls._instance = cls(logger)
        cls._instance._out = os.fdopen(sys.stdout.fileno(), mode='wb', buffering=0)
        cls._instance._in = os.fdopen(sys.stdin.fileno(), mode='rb', buffering=0)
        sys.stdin = None
        if redirect_stdout:
            sys.stdout = sys.stderr
        return cls._instance

    def __enter__(self):
        self._loop_fiber = Fiber(target=self._loop)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            return False  # re-raise exception
        if self._loop_fiber.is_alive():
            self._loop_fiber.switch()

    @classmethod
    def watch_ps(cls, ps, callback):
        cls.get().observed_ps[ps.id].append(callback)

    @classmethod
    def watch_all_ps(cls, ps_set, callback):
        ids = [ps.id for ps in ps_set]
        key = tuple(ids)
        cls.get().observed_all_ps[key].append(callback)

    @classmethod
    def watch_task(cls, task, callback):
        cls.get().observed_task[task.id].append(callback)

    @classmethod
    def watch_all_tasks(cls, tasks, callback):
        key = tuple([t.id for t in tasks])
        for t in tasks:
            pair = (key, callback)
            cls.get().observed_all_tasks[t.id].append(pair)

    @classmethod
    def async(cls, func, *args, **kwargs):
        self = cls.get()

        def _f():
            func(*args, **kwargs)
            self._loop_fiber.switch()

        fb = Fiber(target=_f)
        self._fibers.append(fb)

    @classmethod
    def await_ps(cls, ps):
        self = cls.get()
        fb = Fiber.current()

        def _callback(ps):
            self._fibers.append(fb)

        cls.watch_ps(ps, _callback)
        self._loop_fiber.switch()

    @classmethod
    def await_all_ps(cls, ps_set):
        self = cls.get()
        fb = Fiber.current()

        def _callback(pss):
            self._fibers.append(fb)

        cls.watch_all_ps(ps_set, _callback)
        self._loop_fiber.switch()

    @classmethod
    def await_task(cls, task):
        self = cls.get()
        fb = Fiber.current()

        def _callback(ps):
            self._fibers.append(fb)

        cls.watch_task(task, _callback)
        self._loop_fiber.switch()

    @classmethod
    def await_all_tasks(cls, tasks):
        self = cls.get()
        fb = Fiber.current()

        def _callback(ts):
            self._fibers.append(fb)

        cls.watch_all_tasks(tasks, _callback)
        self._loop_fiber.switch()

    def _loop(self):
        self._launch_all_fibers()
        self._submit_all()
        self._logger.debug("start polling")
        t = self._receive_result()
        while t:
            self._exec_callback_for_task(t)
            self._exec_callback_for_all_task(t)
            if isinstance(t, Run):
                ps = t.parameter_set()
                if ps.is_finished():
                    self._exec_callback()
            self._submit_all()
            t = self._receive_result()

    def _default_logger(self):
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

    def _has_callbacks(self):
        return (len(self.observed_ps) + len(self.observed_all_ps)) > 0

    def _has_unfinished_tasks(self):
        for r in Task.all()[:self.max_submitted_task_id]:
            if not r.is_finished():
                return True
        return False

    def _submit_all(self):
        tasks_to_be_submitted = [t for t in Task.all()[self.max_submitted_task_id:] if not t.is_finished()]
        self._logger.debug("submitting %d Tasks" % len(tasks_to_be_submitted))
        self._print_tasks(tasks_to_be_submitted)
        self.max_submitted_task_id = len(Task.all())

    def _print_tasks(self, tasks):
        b_tasks = [ {"id": t.id, "cmd": t.command} for t in tasks ]
        packed = msgpack.packb(b_tasks)
        size_b = struct.pack('>q', len(packed))
        self._out.write(size_b)
        self._out.write(packed)

    def _launch_all_fibers(self):
        while self._fibers:
            f = self._fibers.pop(0)
            self._logger.debug("starting fiber")
            f.switch()

    def _exec_callback(self):
        while self._check_completed_ps() or self._check_completed_ps_all():
            pass

    def _check_completed_ps(self):
        executed = False
        for psid in list(self.observed_ps.keys()):
            callbacks = self.observed_ps[psid]
            ps = ParameterSet.find(psid)
            while ps.is_finished() and len(callbacks) > 0:
                self._logger.debug("executing callback for ParameterSet %d" % ps.id)
                f = callbacks.pop(0)
                f(ps)
                self._launch_all_fibers()
                executed = True
        empty_keys = [k for k, v in self.observed_ps.items() if len(v) == 0]
        for k in empty_keys:
            self.observed_ps.pop(k)
        return executed

    def _check_completed_ps_all(self):
        executed = False
        for psids in list(self.observed_all_ps.keys()):
            pss = [ParameterSet.find(psid) for psid in psids]
            callbacks = self.observed_all_ps[psids]
            while len(callbacks) > 0 and all([ps.is_finished() for ps in pss]):
                self._logger.debug("executing callback for ParameterSet %s" % repr(psids))
                f = callbacks.pop(0)
                f(pss)
                self._launch_all_fibers()
                executed = True
        empty_keys = [k for k, v in self.observed_all_ps.items() if len(v) == 0]
        for k in empty_keys: self.observed_all_ps.pop(k)
        return executed

    def _exec_callback_for_task(self, task):
        executed = False
        callbacks = self.observed_task[task.id]
        while len(callbacks) > 0:
            self._logger.debug("executing callback for Task %d" % task.id)
            f = callbacks.pop(0)
            f(task)
            self._launch_all_fibers()
            executed = True
        self.observed_task.pop(task.id)
        return executed

    def _exec_callback_for_all_task(self, task):
        executed = False
        callback_pairs = self.observed_all_tasks[task.id]
        to_be_removed = []
        for (idx, pair) in enumerate(callback_pairs):
            task_ids = pair[0]
            if all([Task.find(t).is_finished() for t in task_ids]):
                self._logger.debug("executing callback for Tasks %s" % str(task_ids))
                f = pair[1]
                f(Task.find(t) for t in task_ids)
                to_be_removed.append(idx)
                self._launch_all_fibers()
                executed = True
        for idx in to_be_removed:
            callback_pairs.pop(idx)
        if len(callback_pairs) == 0:
            self.observed_all_tasks.pop(task.id)
        return executed

    def _receive_result(self):
        size_b = self._in.read(8)
        size = struct.unpack('>q', size_b)[0]
        if size == 0: return None
        data_b = self._in.read(size)
        self._logger.debug("received: %s bytes" % size)
        unpacked = msgpack.unpackb(data_b)
        self._logger.debug("received: %s" % str(unpacked))
        tid = unpacked["id"]
        rc = unpacked["rc"]
        place_id = unpacked["rank"]
        start_at = unpacked["start_at"]
        finish_at = unpacked["finish_at"]
        output = unpacked["output"]
        t = Task.find(tid)
        t.store_result(output, rc, place_id, start_at, finish_at)
        self._logger.debug("stored result of Task %d" % tid)
        return t

    def _debug(self):
        sys.stderr.write(str(self.observed_ps) + "\n")
        sys.stderr.write(str(self.observed_all_ps) + "\n")
