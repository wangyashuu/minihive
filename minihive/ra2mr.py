import hashlib
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

s = 20

relation_size = {
    "REGION": s,
    "NATION": s,
    "ORDERS": s,
    "CUSTOMER": s,
    "LINEITEM": s,
}


def hash_to_buckets(val, size):
    val = str(val).encode("utf-8")
    hash = int(hashlib.sha256(val).hexdigest(), 16)
    return hash % size


def match_keys(keys_array):
    items = [keys for keys in keys_array if [None] not in keys]
    if len(items) < 1:
        return False
    both_keys = items[0]
    for keys in keys_array:
        if keys != both_keys and (
            len([k for k in keys if k == [None]]) > 1
            or not all(
                [a == b or a == [None] for a, b in zip(keys, both_keys)]
            )
        ):
            return False
    return True


def decomposite_conjunctive_cond(cond):
    conds = []
    while cond.op is sym.AND:
        cond, right = cond.inputs
        conds.append(right)
    conds.append(cond)
    conds.reverse()
    return conds


def target_of_cond(relation, json_tuple, cond):
    for inp in cond.inputs:
        if isinstance(inp, radb.ast.AttrRef):
            if inp.rel is None:
                return json_tuple[f"{relation}.{inp.name}"]
            elif inp.rel is not None and f"{inp.rel}.{inp.name}" in json_tuple:
                return json_tuple[f"{inp.rel}.{inp.name}"]
    return None


def targets_of_cond(relation, json_tuple, cond):
    vals = []
    for inp in cond.inputs:
        val = None
        if isinstance(inp, radb.ast.Literal):  # isinstance
            val = ast.literal_eval(inp.val)
        if isinstance(inp, radb.ast.AttrRef):
            if inp.rel is None:
                val = json_tuple[f"{relation}.{inp.name}"]
            elif inp.rel is not None and f"{inp.rel}.{inp.name}" in json_tuple:
                val = json_tuple[f"{inp.rel}.{inp.name}"]
        vals.append(val)
    return vals


def target_of_conds(relation, json_tuple, conds):
    return [
        [
            target_of_cond(relation, json_tuple, c)
            for c in decomposite_conjunctive_cond(cond)
        ]
        for cond in conds
    ]


def match_cond(relation, json_tuple, cond):
    left, right = targets_of_cond(relation, json_tuple, cond)
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


def is_map_only_job(task):
    if (
        isinstance(task, luigi.contrib.hadoop.JobTask)
        and task.mapper is not NotImplemented
        and task.reducer is NotImplemented
    ):
        return True

    return False


def get_attr_rels(cond, schema=None):
    if schema is None:
        return set([i.rel for i in cond.inputs if type(i) == radb.ast.AttrRef])


def get_rels(rel):
    if type(rel) == radb.ast.RelRef:
        return set([rel.rel])
    rel_list = [get_rels(i) for i in rel.inputs]
    return set.union(*rel_list)


def optimize_task(cls):
    class Wrapper(cls):  # cls
        optimize_join = False

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.unwrap_requires = super().requires
            self.unwrap_mapper = super().mapper

            # chain fold
            ## rule 1. cat previous map only jobs with mapper.
            branches = dict()
            if self.optimize:
                queue = [] + self.unwrap_requires()
                subtasks = []
                while len(queue) > 0:
                    task = queue.pop()
                    if is_map_only_job(task):
                        raquery = radb.parse.one_statement_from_string(
                            task.querystring
                        )
                        rels = str(sorted(get_rels(raquery)))
                        branches[rels] = [task] + branches.get(rels, [])
                        queue += task.unwrap_requires()
                    else:
                        subtasks.append(task)
            else:
                subtasks = self.unwrap_requires()
            self.branches = branches
            self.subtasks = subtasks

            # 3-ways joins
            if (
                self.optimize
                and len(self.subtasks) == 2
                and self.optimize_join
            ):
                subtasks = []
                subjoin = None
                for t in self.subtasks:
                    if isinstance(t, RelAlgQueryTask):
                        subsubtasks = t.requires()
                        if len(subsubtasks) == 2:
                            subtasks += subsubtasks
                            subjoin = t
                            self.branches.update(t.branches)
                            continue
                    subtasks.append(t)
                if len(subtasks) == 3:
                    self.subjoin = subjoin
                    self.subtasks = subtasks
                    self.unwrap_mapper = self.three_way_joins_mapper
                    self.reducer = self.three_way_joins_reducer

        def requires(self):
            return self.subtasks

        def mapper(self, line):
            lines = [line]
            relation, tuple = line.split("\t")
            branches = [v for k, v in self.branches.items() if relation in k]
            if len(branches) > 0:
                merged_subtasks = branches[0]
                for t in merged_subtasks:
                    new_lines = []
                    for l in lines:
                        new_lines += ["\t".join(o) for o in t.unwrap_mapper(l)]
                    lines = new_lines

            for l in lines:
                for out in self.unwrap_mapper(l):
                    yield out

        def three_way_joins_mapper(self, line):
            relation, tuple = line.split("\t")
            json_tuple = json.loads(tuple)
            conditions = [
                radb.parse.one_statement_from_string(q).cond
                for q in [self.subjoin.querystring, self.querystring]
            ]
            rels_array = [get_attr_rels(cond) for cond in conditions]
            rels = [
                next(
                    iter(cur.difference(*[r for r in rels_array if r != cur]))
                )
                for cur in rels_array
            ]
            keys = target_of_conds(relation, json_tuple, conditions)
            empty_indices = [
                i for i, key in enumerate(keys) if all(k is None for k in key)
            ]

            if len(empty_indices) > 0:
                idx = empty_indices[0]
                bucket_size = relation_size[rels[idx]]

                for i in range(bucket_size):
                    output_key = [
                        hash_to_buckets(key, relation_size[r])
                        if j != idx
                        else i
                        for j, (r, key) in enumerate(zip(rels, keys))
                    ]
                    yield (
                        json.dumps(output_key),
                        json.dumps([relation, json_tuple]),
                    )

            else:
                output_key = [
                    hash_to_buckets(key, relation_size[r])
                    for r, key in zip(rels, keys)
                ]
                yield (
                    json.dumps(output_key),
                    json.dumps([relation, json_tuple]),
                )

        def three_way_joins_reducer(self, key, values):
            conditions = [
                radb.parse.one_statement_from_string(q).cond
                for q in [self.subjoin.querystring, self.querystring]
            ]

            data = [json.loads(val_str) for val_str in values]
            rel_names = {item[0] for item in data}
            keys = json.loads(key)
            if len(rel_names) == len(keys) + 1:
                relations = {
                    rel: [v for r, v in data if r == rel] for rel in rel_names
                }
                output_key = str(sorted(rel_names))
                one, two, three = list(relations.values())
                for one_t in one:
                    for two_t in two:
                        for three_t in three:
                            compare_keys = [
                                target_of_conds(
                                    relation, json_tuple, conditions
                                )
                                for relation, json_tuple in zip(
                                    rel_names, [one_t, two_t, three_t]
                                )
                            ]
                            if match_keys(compare_keys):
                                obj = dict()
                                obj.update(one_t)
                                obj.update(two_t)
                                obj.update(three_t)
                                yield (output_key, json.dumps(obj))

    return Wrapper


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
    optimize = luigi.BoolParameter()

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


def task_factory(raquery, step=1, env=ExecEnv.HDFS, optimize=False):
    assert isinstance(raquery, radb.ast.Node)

    if isinstance(raquery, radb.ast.Select):
        return SelectTask(
            querystring=str(raquery) + ";",
            step=step,
            exec_environment=env,
            optimize=optimize,
        )

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(
            querystring=str(raquery) + ";",
            step=step,
            exec_environment=env,
            optimize=optimize,
        )

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(
            querystring=str(raquery) + ";",
            step=step,
            exec_environment=env,
            optimize=optimize,
        )

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(
            querystring=str(raquery) + ";",
            step=step,
            exec_environment=env,
            optimize=optimize,
        )

    else:
        # We will not evaluate the Cross product on Hadoop, too expensive.
        raise Exception(
            "Operator " + str(type(raquery)) + " not implemented (yet)."
        )


@optimize_task
class JoinTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Join)

        task1 = task_factory(
            raquery.inputs[0],
            step=self.step + 1,
            env=self.exec_environment,
            optimize=self.optimize,
        )
        task2 = task_factory(
            raquery.inputs[1],
            step=self.step + count_steps(raquery.inputs[0]) + 1,
            env=self.exec_environment,
            optimize=self.optimize,
        )

        return [task1, task2]

    def mapper(self, line):
        relation, tuple = line.split("\t")
        json_tuple = json.loads(tuple)
        raquery = radb.parse.one_statement_from_string(self.querystring)
        conditions = [raquery.cond]
        keys = target_of_conds(relation, json_tuple, conditions)
        yield (
            json.dumps(keys),
            json.dumps([relation, json_tuple]),
        )

    def reducer(self, key, values):
        data = [json.loads(val_str) for val_str in values]
        rel_names = {item[0] for item in data}
        keys = json.loads(key)

        if len(rel_names) == len(keys) + 1:
            relations = {
                rel: [v for r, v in data if r == rel] for rel in rel_names
            }
            one, two = list(relations.values())
            for one_t in one:
                for two_t in two:
                    obj = dict()
                    obj.update(one_t)
                    obj.update(two_t)
                    yield (str(sorted(rel_names)), json.dumps(obj))


@optimize_task
class SelectTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Select)

        return [
            task_factory(
                raquery.inputs[0],
                step=self.step + 1,
                env=self.exec_environment,
                optimize=self.optimize,
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


@optimize_task
class RenameTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Rename)

        return [
            task_factory(
                raquery.inputs[0],
                step=self.step + 1,
                env=self.exec_environment,
                optimize=self.optimize,
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


@optimize_task
class ProjectTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Project)

        return [
            task_factory(
                raquery.inputs[0],
                step=self.step + 1,
                env=self.exec_environment,
                optimize=self.optimize,
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
        # TODO: replace key with relation name
        yield (key, next(values))

        """ ...................... fill in your code above ........................"""


if __name__ == "__main__":
    luigi.run()
