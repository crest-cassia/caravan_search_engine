from collections import OrderedDict
import json
from . import tables


class Task:
    def __init__(self, task_id, command):
        self.id = task_id
        if command is not None:
            self.command = command
        self.rc = None
        self.place_id = None
        self.start_at = None
        self.finish_at = None
        self.results = None

    @classmethod
    def create(cls, cmd):
        tab = tables.Tables.get()
        next_id = len(tab.tasks_table)
        t = cls(next_id, cmd)
        tab.tasks_table.append(t)
        return t

    def is_finished(self):
        return not (self.rc is None)

    def store_result(self, results, rc, place_id, start_at, finish_at):
        self.results = tuple(results)
        self.rc = rc
        self.place_id = place_id
        self.start_at = start_at
        self.finish_at = finish_at

    def to_dict(self):
        o = OrderedDict()
        o["id"] = self.id
        o["command"] = self.command
        if self.rc is not None:
            o["rc"] = self.rc
            o["place_id"] = self.place_id
            o["start_at"] = self.start_at
            o["finish_at"] = self.finish_at
            o["results"] = self.results
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
        import struct
        with open(path, 'wb') as f:
            for t in cls.all():
                print(t.dumps())
                num_results = len(t.results)
                fmt = f">6q{num_results:d}d"
                bytes = struct.pack(fmt, t.id, t.rc, t.place_id, t.start_at, t.finish_at, len(t.results), *t.results)
                f.write(bytes)
            f.flush()

