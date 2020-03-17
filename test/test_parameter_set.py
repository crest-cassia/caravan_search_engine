import unittest
from caravan.tables import Tables
from caravan.parameter_set import ParameterSet


class ParameterSetTest(unittest.TestCase):
    def setUp(self):
        self.t = Tables.get()
        self.t.clear()

    def test_ps(self):
        ps = ParameterSet(500, (2, 3, 4, 5))
        self.assertEqual(ps.id, 500)
        self.assertEqual(ps.params, (2, 3, 4, 5))
        self.assertEqual(ps.run_ids, [])

    def test_find_or_create(self):
        ps = ParameterSet.find_or_create(0, 1, 2, 3)
        self.assertEqual(ps.id, 0)
        self.assertEqual(ps.params, (0, 1, 2, 3))
        self.assertEqual(len(ParameterSet.all()), 1)
        ps2 = ParameterSet.find_or_create(3, 4, 5, 6)
        self.assertEqual(len(ParameterSet.all()), 2)

    def test_find_or_create(self):
        ps1 = ParameterSet.find_or_create(0, 1, 2, 3)
        ps2 = ParameterSet.find_or_create(0, 1, 2, 3)
        self.assertEqual(ps1, ps2)
        self.assertEqual(len(ParameterSet.all()), 1)

    def test_create_runs(self):
        ps = ParameterSet.find_or_create(0, 1, 2, 3)
        runs = ps.create_runs_upto(3)
        self.assertEqual([r.id for r in runs], [0, 1, 2])
        self.assertEqual([r.seed for r in runs], [0, 1, 2])
        ps2 = ParameterSet.find_or_create(0, 1, 3, 4)
        runs = ps2.create_runs_upto(3)
        self.assertEqual([r.id for r in runs], [3, 4, 5])
        self.assertEqual([r.seed for r in runs], [0, 1, 2])

    def test_is_finished(self):
        ps = ParameterSet.find_or_create(0, 1, 2, 3)
        self.assertEqual(ps.is_finished(), True)
        runs = ps.create_runs_upto(1)
        self.assertFalse(ps.is_finished())
        self.assertEqual(len(ps.finished_runs()), 0)
        runs[0].store_result([1.0, 2.0, 3.0], 0, 3, 111, 222)
        self.assertTrue(ps.is_finished())
        self.assertEqual(len(ps.finished_runs()), 1)

    def test_outputs(self):
        ps = ParameterSet.find_or_create(0, 1, 2, 3)
        runs = ps.create_runs_upto(3)
        self.assertEqual(ps.outputs(), [])
        for (i, r) in enumerate(runs):
            r.store_result([1.0 + i, 2.0 + i, 3.0 + i], 0, 3, 111, 222)
        self.assertEqual(ps.outputs(), [ [1.0,2.0,3.0],[2.0,3.0,4.0],[3.0,4.0,5.0] ] )

    def test_all(self):
        ps = ParameterSet.find_or_create(0, 1, 2, 3)
        self.assertEqual(ParameterSet.all(), [ps])
        ps2 = ParameterSet.find_or_create(0, 1, 2, 4)
        self.assertEqual(ParameterSet.all(), [ps, ps2])
        self.assertEqual(len(ParameterSet.all()), 2)

    def test_find(self):
        ps = ParameterSet.find_or_create(0, 1, 2, 3)
        ps2 = ParameterSet.find_or_create(0, 1, 2, 4)
        pid = ps2.id
        self.assertEqual(pid, 1)
        self.assertEqual(ParameterSet.find(1), ps2)


if __name__ == '__main__':
    unittest.main()
