from .server import Server

class EventQueue:

    def __init__(self, num_places):
        self.n = num_places
        self.running = [None for i in range(self.n)]
        self.t = 0
        self.tasks = []

    def push_all(self, tasks):
        self.tasks.extend(tasks)

    def pop(self):
        while None in self.running and len(self.tasks)>0:
            idx = self.running.index(None)
            starting = self.tasks.pop(0)
            starting.start_at = self.t
            starting.finish_at = self.t + starting.dt
            starting.place_id = idx
            self.running[idx] = starting

        compacted = [r for r in self.running if r is not None]
        if len(compacted) == 0:
            return None
        else:
            next_task = min(compacted, key=(lambda r: r.finish_at))
            self.t = next_task.finish_at
            idx = self.running.index(next_task)
            self.running[idx] = None
            return next_task

_queue = None
_stub_simulator = None

def start_stub(stub_simulator, num_proc = 1, logger = None):
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
        print("receive result is called")
        t = _queue.pop()
        if t is None:
            return None
        t.rc = 0
        print(t.dumps())
        return t
    Server._receive_result = receive_result_stub
    return Server._instance

