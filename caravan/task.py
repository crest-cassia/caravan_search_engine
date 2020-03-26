from collections import OrderedDict
import json
from . import tables


class Task:
    def __init__(self, task_id, command, input_json = None):
        self.id = task_id
        if command is not None:
            self.command = command
        self.input = input_json
        self.rc = None
        self.rank = None
        self.start_at = None
        self.finish_at = None
        self.output = None

    @classmethod
    def create(cls, cmd, input_json = None):
        tab = tables.Tables.get()
        next_id = len(tab.tasks_table)
        t = cls(next_id, cmd, input_json)
        tab.tasks_table.append(t)
        return t

    def is_finished(self):
        return not (self.rc is None)

    def store_result(self, output, rc, rank, start_at, finish_at):
        self.output = output
        self.rc = rc
        self.rank = rank
        self.start_at = start_at
        self.finish_at = finish_at

    def to_dict(self):
        o = OrderedDict()
        o["id"] = self.id
        o["command"] = self.command
        o["input"] = self.input
        if self.rc is not None:
            o["rc"] = self.rc
            o["rank"] = self.rank
            o["start_at"] = self.start_at
            o["finish_at"] = self.finish_at
            o["output"] = self.output
        return o

    def add_callback(self, f):
        from .server import Server
        Server.watch_task(self, f)

    @classmethod
    def all(cls):
        return tables.Tables.get().tasks_table

    @classmethod
    def find(cls, id):
        return tables.Tables.get().tasks_table[id]

    def dumps(self):
        return json.dumps(self.to_dict())

    @classmethod
    def dump_binary(cls, path):
        import msgpack
        with open(path, 'wb') as f:
            def _task_to_obj(t):
                return {"id": t.id, "rc": t.rc, "rank": t.rank, "start_at": t.start_at, "finish_at": t.finish_at, "output": t.output}
            task_results = { t.id:_task_to_obj(t) for t in cls.all() if t.rc == 0 }
            b = msgpack.packb( task_results )
            f.write(b)
            f.flush()
