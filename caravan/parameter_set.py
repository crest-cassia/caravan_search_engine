from collections import OrderedDict
from . import run
from . import tables


class ParameterSet:
    command_func = None

    def __init__(self, ps_id, params):
        self.id = ps_id
        self.params = params
        self.run_ids = []

    @classmethod
    def set_command_func(cls, f):
        cls.command_func = f

    def make_command(self, seed):
        f = self.__class__.command_func
        return f(self.params, seed)

    @classmethod
    def find_or_create(cls, *params):
        t = tables.Tables.get()
        prm = tuple(params)
        if len(params) == 1 and isinstance(params[0], tuple):
            prm = params[0]
        if prm in t.param_ps_dict:
            return t.param_ps_dict[prm]
        else:
            next_id = len(t.ps_table)
            ps = cls(next_id, prm)
            t.ps_table.append(ps)
            t.param_ps_dict[prm] = ps
            return ps

    def create_runs(self, num_runs):
        created = [run.Run.create(self) for _ in range(num_runs)]
        return created

    def create_runs_upto(self, target_num):
        current = len(self.run_ids)
        if target_num > current:
            self.create_runs(target_num - current)
        return self.runs()[:target_num]

    def runs(self):
        return [tables.Tables.get().tasks_table[rid] for rid in self.run_ids]

    def finished_runs(self):
        return [r for r in self.runs() if r.is_finished()]

    def is_finished(self):
        return all([r.is_finished() for r in self.runs()])

    def average_results(self):
        runs = [r for r in self.finished_runs() if r.rc == 0]
        if len(runs) == 0:
            return ()
        else:
            n = len(runs[0].results)
            averages = []
            for i in range(n):
                results = [r.results[i] for r in runs]
                avg = sum(results) / len(results)
                averages.append(avg)
            return tuple(averages)

    def to_dict(self):
        o = OrderedDict()
        o["id"] = self.id
        o["params"] = self.params
        o["run_ids"] = self.run_ids
        return o

    def completion(self):
        from .server import Server
        return Server.ps_completion(self)

    @classmethod
    def all(cls):
        return tables.Tables.get().ps_table

    @classmethod
    def find(cls, id):
        return tables.Tables.get().ps_table[id]

    def dumps(self):
        runs_str = ",\n".join(["    " + r.dumps() for r in self.runs()])
        return "{\"id\": %d, \"params\": %s, \"runs\": [\n%s\n]}" % (self.id, str(self.params), runs_str)
