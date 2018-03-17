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
        t = tables.Tables.get()
        next_id = len(t.tasks_table)
        r = cls(next_id, cmd)
        t.tasks_table.append(r)

    def is_finished(self):
        return not (self.rc is None)

    def store_result(self, results, rc, place_id, start_at, finish_at):
        self.results = results
        self.rc = rc
        self.place_id = place_id
        self.start_at = start_at
        self.finish_at = finish_at

    def to_dict(self):
        o = OrderedDict()
        o["id"] = self.id
        o["command"] = self.command
        if self.rc:
            o["rc"] = self.rc
            o["place_id"] = self.place_id
            o["start_at"] = self.start_at
            o["finish_at"] = self.finish_at
            o["results"] = self.results
        return o

    @classmethod
    def all(cls):
        return tables.Tables.get().tasks_table

    @classmethod
    def find(cls,id):
        return tables.Tables.get().tasks_table[id]

    def dumps(self):
        return json.dumps(self.to_dict())

