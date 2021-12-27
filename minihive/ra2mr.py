from enum import Enum
import ast
import operator
import json

import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
import radb.parse
from radb.parse import RAParser as sym

"""
util function
"""


def decomposite_conjunctive_cond(cond):
    conds = []
    while cond.op is sym.AND:
        cond, right = cond.inputs
        conds.append(right)
    conds.append(cond)
    conds.reverse()
    return conds


def get_cond_target(relation, json, target):
    if isinstance(target, radb.ast.Literal):  # isinstance
        return ast.literal_eval(target.val)

    if isinstance(target, radb.ast.AttrRef):
        if target.rel is None or target.rel == relation:
            return json[f"{relation}.{target.name}"]
        elif target.rel is not None and f"{target.rel}.{target.name}" in json:
            return json[f"{target.rel}.{target.name}"]
    return None


def match_cond(relation, json, cond):
    left, right = cond.inputs
    left = get_cond_target(relation, json, left)
    right = get_cond_target(relation, json, right)
    comparison_ops = {
        sym.LT: operator.lt,  # "<"
        sym.LE: operator.le,  # "<="
        sym.EQ: operator.eq,  # "="
        sym.NE: operator.ne,  # "!="
        sym.GE: operator.ge,  # ">="
        sym.GT: operator.gt,  # ">"
    }
    op = comparison_ops[cond.op]
    return op(left, right)


"""
Control where the input data comes from, and where output data should go.
"""


class ExecEnv(Enum):
    LOCAL = 1  # read/write local files
    HDFS = 2  # read/write HDFS
    MOCK = 3  # read/write mock data to an in-memory file system.


"""
Switches between different execution environments and file systems.
"""


class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)

    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK:
            return MockTarget(fn)
        else:
            return luigi.LocalTarget(fn)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        return self.get_output(self.filename)


"""
Counts the number of steps / luigi tasks that we need for evaluating this query.
"""


def count_steps(raquery):
    assert isinstance(raquery, radb.ast.Node)

    if (
        isinstance(raquery, radb.ast.Select)
        or isinstance(raquery, radb.ast.Project)
        or isinstance(raquery, radb.ast.Rename)
    ):
        return 1 + count_steps(raquery.inputs[0])

    elif isinstance(raquery, radb.ast.Join):
        return (
            1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])
        )

    elif isinstance(raquery, radb.ast.RelRef):
        return 1

    else:
        raise Exception(
            "count_steps: Cannot handle operator " + str(type(raquery)) + "."
        )


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    """
    Each physical operator knows its (partial) query string.
    As a string, the value of this parameter can be searialized
    and shipped to the data node in the Hadoop cluster.
    """

    querystring = luigi.Parameter()

    """
    Each physical operator within a query has its own step-id.
    This is used to rename the temporary files for exhanging
    data between chained MapReduce jobs.
    """
    step = luigi.IntParameter(default=1)

    """
    In HDFS, we call the folders for temporary data tmp1, tmp2, ...
    In the local or mock file system, we call the files tmp1.tmp...
    """

    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)


"""
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
"""


def task_factory(raquery, step=1, env=ExecEnv.HDFS):

    assert isinstance(raquery, radb.ast.Node)

    if isinstance(raquery, radb.ast.Select):
        return SelectTask(
            querystring=str(raquery) + ";", step=step, exec_environment=env
        )

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(
            querystring=str(raquery) + ";", step=step, exec_environment=env
        )

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(
            querystring=str(raquery) + ";", step=step, exec_environment=env
        )

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(
            querystring=str(raquery) + ";", step=step, exec_environment=env
        )

    else:
        # We will not evaluate the Cross product on Hadoop, too expensive.
        raise Exception(
            "Operator " + str(type(raquery)) + " not implemented (yet)."
        )


class JoinTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Join)

        task1 = task_factory(
            raquery.inputs[0], step=self.step + 1, env=self.exec_environment
        )
        task2 = task_factory(
            raquery.inputs[1],
            step=self.step + count_steps(raquery.inputs[0]) + 1,
            env=self.exec_environment,
        )

        return [task1, task2]

    def mapper(self, line):
        relation, tuple = line.split("\t")
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)
        condition = raquery.cond

        """ .................. fill in your code below ...................."""
        conds = decomposite_conjunctive_cond(condition)
        key = [
            get_cond_target(relation, json_tuple, c.inputs[0])
            or get_cond_target(relation, json_tuple, c.inputs[1])
            for c in conds
        ]

        yield (json.dumps(key), json.dumps({relation: json_tuple}))

        """ .................. fill in your code above ...................."""

    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)

        """ ................. fill in your code below ..................."""

        relations = dict()
        for val_str in values:
            val = json.loads(val_str)
            rel, obj = next(iter(val.items()))
            relations[rel] = relations.get(rel, []) + [obj]

        if len(relations) == 2:
            left, right = list(relations.values())
            for left_t in left:
                for right_t in right:
                    obj = dict()
                    obj.update(left_t)
                    obj.update(right_t)
                    yield ("foo", json.dumps(obj))
                    # print("for debug")

        """ ................. fill in your code above ..................."""


class SelectTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Select)

        return [
            task_factory(
                raquery.inputs[0],
                step=self.step + 1,
                env=self.exec_environment,
            )
        ]

    def mapper(self, line):
        relation, tuple = line.split("\t")
        json_tuple = json.loads(tuple)

        condition = radb.parse.one_statement_from_string(self.querystring).cond
        """ .................. fill in your code below ....................."""
        conds = decomposite_conjunctive_cond(condition)
        if all(match_cond(relation, json_tuple, c) for c in conds):
            yield (relation, tuple)
        """ .................. fill in your code above ...................."""


class RenameTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Rename)

        return [
            task_factory(
                raquery.inputs[0],
                step=self.step + 1,
                env=self.exec_environment,
            )
        ]

    def mapper(self, line):
        relation, tuple = line.split("\t")
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)

        """ .................. fill in your code below ...................."""
        relname = raquery.relname
        obj = {
            key.replace(relation, relname): val
            for key, val in json_tuple.items()
        }
        yield (relname, json.dumps(obj))

        """ .................. fill in your code above ...................."""


class ProjectTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Project)

        return [
            task_factory(
                raquery.inputs[0],
                step=self.step + 1,
                env=self.exec_environment,
            )
        ]

    def mapper(self, line):
        relation, tuple = line.split("\t")
        json_tuple = json.loads(tuple)

        attrs = radb.parse.one_statement_from_string(self.querystring).attrs

        """ ...................... fill in your code below ........................"""
        obj = dict()
        for a in attrs:
            key = f"{a.rel or relation}.{a.name}"
            obj[key] = json_tuple[key]

        yield (json.dumps(obj), json.dumps(obj))

        """ ...................... fill in your code above ........................"""

    def reducer(self, key, values):
        """...................... fill in your code below ........................"""

        yield (key, next(values))

        """ ...................... fill in your code above ........................"""


if __name__ == "__main__":
    luigi.run()
