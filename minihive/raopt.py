from radb.ast import (
    Cross,
    Project,
    Rename,
    Join,
    RelRef,
    Select,
    AttrRef,
    ValExprBinaryOp,
)
from radb.parse import RAParser as sym


def rule_break_up_selections(rel):
    def operate_select(rel):
        # TODO: Select in select
        cond = rel.cond
        obj = rel.inputs[0]
        while cond.op is sym.AND:
            left, right = cond.inputs
            obj = Select(right, obj)
            cond = left
        return Select(cond, obj)

    return traverse_until_select(operate_select, rel)


def rule_push_down_selections(rel, schema):
    def operate_select(rel):
        target, cond = rel.inputs[0], rel.cond
        cond, target = push_down_selections(target, cond, schema)
        if cond is None:
            return target
        else:
            return Select(cond, target)

    return traverse_until_select(operate_select, rel)


def rule_merge_selections(rel):
    def operate_select(rel):
        target = rel.inputs[0]
        cond = rel.cond

        if type(target) == Select:
            cond = get_conjunctive_cond(
                [cond] + decomposite_conjunctive_cond(target.cond)
            )
            return Select(cond, target.inputs[0])
        return Select(cond, target)

    return traverse_until_select(operate_select, rel)


def rule_introduce_joins(rel):
    def operate_select(rel):
        target, cond = rel.inputs[0], rel.cond
        rels = get_rels(target)
        if len(rels) < 2:
            return rel

        conds = decomposite_conjunctive_cond(cond)
        join_conds, left_conds = [], []
        for c in conds:
            attr_rels = get_attr_rels(c)
            if len(attr_rels) == 2 and attr_rels <= rels and c.op == sym.EQ:
                join_conds.append(c)
            else:
                left_conds.append(c)

        join_cond = get_conjunctive_cond(join_conds)
        if join_cond is not None:
            join_cond, target = introduce_join(join_cond, target)
            if join_cond is None:
                left_cond = get_conjunctive_cond(left_conds)
                if left_cond is None:
                    return target
                return Select(left_cond, target)
        return Select(cond, target)

    return traverse_until_select(operate_select, rel)


def traverse_until_select(operate_select, rel, *args):
    def get_args(target):
        return [operate_select, target] + list(args)

    t = type(rel)
    if t == Cross:
        left, right = rel.inputs
        left = traverse_until_select(*get_args(left))
        right = traverse_until_select(*get_args(right))
        return Cross(left, right)
    elif t == Project:
        target_rel = traverse_until_select(*get_args(rel.inputs[0]))
        return Project(rel.attrs, target_rel)
    elif t == Rename:
        target_rel = traverse_until_select(*get_args(rel.inputs[0]))
        return Rename(rel.relname, rel.attrnames, target_rel)
    elif t == Join:
        left, right = rel.inputs
        left = traverse_until_select(*get_args(left))
        right = traverse_until_select(*get_args(right))
        return Join(left, rel.cond, right)
    elif t == RelRef:
        return rel
    elif t == Select:
        rel_input = traverse_until_select(*get_args(rel.inputs[0]))
        return operate_select(Select(rel.cond, rel_input))


def push_down_selections(rel, cond, schema):
    t = type(rel)
    attr_rels = get_attr_rels(cond, schema)
    if t == Cross:
        left_rel, right_rel = rel.inputs
        rels = get_rels(rel)
        if attr_rels < rels:
            cond, potential_left = push_down_selections(left_rel, cond, schema)
            if cond is None:
                return None, Cross(potential_left, right_rel)
            cond, potential_right = push_down_selections(
                right_rel, cond, schema
            )
            if cond is None:
                return None, Cross(left_rel, potential_right)
            return None, Select(cond, rel)
        elif attr_rels == rels:
            left_rels = get_rels(left_rel)
            right_rels = get_rels(right_rel)
            if attr_rels <= set.union(left_rels, right_rels) and (
                len(left_rels) == 1 or len(right_rels) == 1
            ):
                return None, Select(cond, rel)

    elif t == RelRef or t == Rename:
        rels = get_rels(rel)
        if attr_rels <= rels and len(attr_rels) == 1:
            return None, Select(cond, rel)
    elif t == Select:
        rels = get_rels(rel)
        if attr_rels == rels:
            return None, Select(cond, rel)
        elif len(attr_rels) < len(rels):
            cond, potential = push_down_selections(rel.inputs[0], cond, schema)
            if cond is None:
                return None, Select(rel.cond, potential)
    return cond, rel


def get_attr_rels(cond, schema=None):
    if schema is None:
        return set([i.rel for i in cond.inputs if type(i) == AttrRef])

    def get_rel(attr, schema):
        if attr.rel is not None:
            return attr.rel
        for r in schema:
            for a in schema[r]:
                if a == attr.name:
                    return r
        return None

    return set([get_rel(i, schema) for i in cond.inputs if type(i) == AttrRef])


def get_rels(rel):
    if type(rel) == RelRef:
        return set([rel.rel])
    rel_list = [get_rels(i) for i in rel.inputs]
    if type(rel) == Rename:
        rel_list = [{rel.relname}]
    return set.union(*rel_list)


def decomposite_conjunctive_cond(cond):
    conds = []
    while cond.op is sym.AND:
        cond, right = cond.inputs
        conds.append(right)
    conds.append(cond)
    conds.reverse()
    return conds


def get_conjunctive_cond(conds):
    if len(conds) < 1:
        return None
    res = conds[0]
    for c in conds[1:]:
        res = ValExprBinaryOp(res, sym.AND, c)
    return res


def introduce_join(cond, rel):
    t = type(rel)
    if t == Cross:
        attr_rels = get_attr_rels(cond)

        left_rel, right_rel = rel.inputs
        left_rels = get_rels(left_rel)
        right_rels = get_rels(right_rel)

        if attr_rels <= set.union(left_rels, right_rels) and (
            len(left_rels) == 1 or len(right_rels) == 1
        ):
            return None, Join(left_rel, cond, right_rel)

    elif t == Project:
        cond, target_rel = introduce_join(rel.inputs[0])
        if cond is None:
            return None, Project(rel.attrs, target_rel)
    elif t == Rename:
        cond, target_rel = introduce_join(rel.inputs[0])
        if cond is None:
            return None, Rename(rel.relname, rel.attrnames, target_rel)

    return cond, rel
