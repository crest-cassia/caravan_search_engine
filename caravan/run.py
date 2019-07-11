from collections import OrderedDict
import json
from . import tables
from .task import Task


class Run(Task):
    def __init__(self, run_id, ps_id, seed):
        super().__init__(run_id, None)
        self.ps_id = ps_id
        self.seed = seed

    @classmethod
    def create(cls, ps):
        t = tables.Tables.get()
        next_seed = len(ps.run_ids)
        next_id = len(t.tasks_table)
        r = cls(next_id, ps.id, next_seed)
        ps.run_ids.append(r.id)
        t.tasks_table.append(r)
        return r

    @property
    def command(self):
        ps = self.parameter_set()
        return ps.make_command(self.seed)

    def parameter_set(self):
        return tables.Tables.get().ps_table[self.ps_id]

    def to_dict(self):
        o = OrderedDict()
        o["id"] = self.id
        o["ps_id"] = self.ps_id
        o["seed"] = self.seed
        if self.rc is not None:
            o["rc"] = self.rc
            o["place_id"] = self.place_id
            o["start_at"] = self.start_at
            o["finish_at"] = self.finish_at
            o["results"] = self.results
        return o

    @classmethod
    def all(cls):
        return [t for t in tables.Tables.get().tasks_table if isinstance(t, cls)]

    @classmethod
    def find(cls, id):
        return tables.Tables.get().tasks_table[id]

    def dumps(self):
        return json.dumps(self.to_dict())
