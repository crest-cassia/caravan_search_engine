from .server import Server
from .task import Task
import bisect


class EventQueue:
    def __init__(self, num_places):
        self.n = num_places
        self.sleeping_places = list(range(self.n))
        self.running_tasks = []
        self.finish_at_list = []
        self.t = 0
        self.tasks = []

    def push_all(self, tasks):
        self.tasks.extend(tasks)

    def pop(self):
        while len(self.sleeping_places) > 0 and len(self.tasks) > 0:
            place = self.sleeping_places.pop(0)
            starting = self.tasks.pop(0)
            starting.start_at = self.t
            starting.finish_at = self.t + starting.dt
            starting.place_id = place
            f = starting.finish_at
            idx = bisect.bisect_right(self.finish_at_list, f)
            self.finish_at_list.insert(idx, f)
            self.running_tasks.insert(idx, starting)

        if len(self.sleeping_places) == self.n:
            return None
        else:
            self.finish_at_list.pop(0)
            next_task = self.running_tasks.pop(0)
            self.t = next_task.finish_at
            p = next_task.place_id
            self.sleeping_places.append(p)
            return next_task


_queue = None
_stub_simulator = None


def start_stub(stub_simulator, num_proc=1, logger=None, dump_path='tasks.bin'):
    global _queue, _stub_simulator
    _stub_simulator = stub_simulator
    Server._instance = Server(logger)
    _queue = EventQueue(num_proc)

    # override the methods
    def print_tasks_stub(self, tasks):
        for t in tasks:
            res, dt = _stub_simulator(t)
            t.results = res
            t.dt = int(1000 * dt)
        _queue.push_all(tasks)

    Server._print_tasks = print_tasks_stub

    def receive_result_stub(self):
        t = _queue.pop()
        if t is None:
            return None
        t.rc = 0
        return t

    Server._receive_result = receive_result_stub

    Server.org_exit = Server.__exit__

    def _exit(self, exc_type, exc_val, exc_tb):
        self.org_exit(exc_type, exc_val, exc_tb)
        Task.dump_binary(dump_path)

    Server.__exit__ = _exit
    return Server._instance
