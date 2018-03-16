import sys,logging,os
from collections import defaultdict
try:
    from fibers import Fiber
except ImportError:
    from .pseudo_fiber import Fiber
from .run import Run
from .parameter_set import ParameterSet

class Server(object):

    _instance = None

    @classmethod
    def get(cls):
        if cls._instance is None:
            raise Exception("use Server.start() method")
        return cls._instance

    def __init__(self, map_func, logger = None):
        self.observed_ps = defaultdict(list)
        self.observed_all_ps = defaultdict(list)
        self.max_submitted_run_id = 0
        self._logger = logger or self._default_logger()
        self.map_func = map_func
        self._fibers = []

    @classmethod
    def start(cls, map_func, logger = None):
        cls._instance = cls(map_func, logger)
        return cls._instance

    def __enter__(self):
        self._loop_fiber = Fiber(target=self._loop)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            return False   # re-raise exception
        if self._loop_fiber.is_alive():
            self._loop_fiber.switch()

    @classmethod
    def watch_ps(cls, ps, callback):
        cls.get().observed_ps[ ps.id ].append(callback)

    @classmethod
    def watch_all_ps(cls, ps_set, callback ):
        ids = [ ps.id for ps in ps_set ]
        key = tuple( ids )
        cls.get().observed_all_ps[key].append(callback)

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
            self._fibsers.append(fb)
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

    def _loop(self):
        self._launch_all_fibers()
        self._submit_all()
        self._logger.debug("start polling")
        r = self._receive_result()
        while r:
            ps = r.parameter_set()
            if ps.is_finished():
                self._exec_callback()
            self._submit_all()
            r = self._receive_result()

    def _default_logger(self):
        logger = logging.getLogger(__name__)
        log_level = logging.INFO
        if 'CARAVAN_SEARCH_ENGINE_LOGLEVEL' in os.environ:
            s = os.environ['CARAVAN_SEARCH_ENGINE_LOGLEVEL']
            levels = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR, 'CRITICAL': logging.CRITICAL}
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
        return ( len( self.observed_ps ) + len( self.observed_all_ps ) ) > 0

    def _has_unfinished_runs(self):
        for r in Run.all()[:self.max_submitted_run_id]:
            if not r.is_finished():
                return True
        return False

    def _submit_all(self):
        runs_to_be_submitted = [r for r in Run.all()[self.max_submitted_run_id:] if not r.is_finished()]
        self._logger.debug("submitting %d Runs" % len(runs_to_be_submitted))
        self._print_tasks(runs_to_be_submitted)
        self.max_submitted_run_id = len(Run.all())

    def _print_tasks(self,runs):
        for r in runs:
            line = "%d %s\n" % (r.id, self.map_func( r.parameter_set().params, r.seed ))
            sys.stdout.write(line)
        sys.stdout.write("\n")

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
            while ps.is_finished() and len(callbacks)>0:
                self._logger.debug("executing callback for ParameterSet %d" % ps.id)
                f = callbacks.pop(0)
                f(ps)
                self._launch_all_fibers()
                executed = True
        empty_keys = [k for k,v in self.observed_ps.items() if len(v)==0 ]
        for k in empty_keys:
            self.observed_ps.pop(k)
        return executed

    def _check_completed_ps_all(self):
        executed = False
        for psids in list(self.observed_all_ps.keys()):
            pss = [ParameterSet.find(psid) for psid in psids]
            callbacks = self.observed_all_ps[psids]
            while len(callbacks)>0 and all([ps.is_finished() for ps in pss]):
                self._logger.debug("executing callback for ParameterSet %s" % repr(psids))
                f = callbacks.pop(0)
                f(pss)
                self._launch_all_fibers()
                executed = True
        empty_keys = [k for k,v in self.observed_all_ps.items() if len(v) == 0]
        for k in empty_keys: self.observed_all_ps.pop(k)
        return executed

    def _receive_result(self):
        line = sys.stdin.readline()
        if not line: return None
        line = line.rstrip()
        self._logger.debug("received: %s" % line)
        if not line: return None
        l = line.split(' ')
        rid,rc,place_id,start_at,finish_at = [ int(x) for x in l[:5] ]
        results = [ float(x) for x in l[5:] ]
        r = Run.find(rid)
        r.store_result( results, rc, place_id, start_at, finish_at )
        self._logger.debug("stored result of Run %d" % rid)
        return r

    def _debug(self):
        sys.stderr.write(str(self.observed_ps)+"\n")
        sys.stderr.write(str(self.observed_all_ps)+"\n")

