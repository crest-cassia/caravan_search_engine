import pickle

class Tables:

    _instance = None

    @classmethod
    def get(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        if self.__class__._instance is not None:
            raise "do not call constructor directly"
        self.ps_table = []
        self.param_ps_dict = {}
        self.tasks_table = []

    def clear(self):
        self.ps_table = []
        self.param_ps_dict = {}
        self.tasks_table = []

    def dumps(self):
        ps_str = ",\n".join([ps.dumps() for ps in self.ps_table])
        return "[\n%s\n]\n" % ps_str

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 2:
        t = Tables.load(sys.argv[1])
        print(t.dumps())
    else:
        sys.stderr.write("[Error] invalid number of arguments\n")
        sys.stderr.write("  Usage: python %s <pickle file>\n")
        sys.stderr.write("    it will print the data to stdout\n")
        raise RuntimeError("Invalid number of arguments")

