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
    t = type(rel)
    if t == Cross:
        left, right = rel.inputs
        left = rule_break_up_selections(left)
        right = rule_break_up_selections(right)
        return Cross(left, right)
    elif t == Project:
        target_rel = rule_break_up_selections(rel.inputs[0])
        return Project(rel.attrs, target_rel)
    elif t == Rename:
        target_rel = rule_break_up_selections(rel.inputs[0])
        return Rename(rel.relname, rel.attrnames, target_rel)
    elif t == Join:
        left, right = rel.inputs
        left = rule_break_up_selections(left)
        right = rule_break_up_selections(right)
        return Join(left, rel.cond, right)
    elif t == RelRef:
        return rel
    elif t == Select:
        # TODO: Select in select
        cond = rel.cond
        obj = rel.inputs[0]
        while cond.op is sym.AND:
            left, right = cond.inputs
            obj = Select(right, obj)
            cond = left
        return Select(cond, obj)


def rule_push_down_selections(rel, schema):
    t = type(rel)
    if t == Cross:
        left, right = rel.inputs
        left = rule_push_down_selections(left, schema)
        right = rule_push_down_selections(right, schema)
        return Cross(left, right)
    elif t == Project:
        target_rel = rule_push_down_selections(rel.inputs[0], schema)
        return Project(rel.attrs, target_rel)
    elif t == Rename:
        target_rel = rule_push_down_selections(rel.inputs[0], schema)
        return Rename(rel.relname, rel.attrnames, target_rel)
    elif t == Join:
        left, right = rel.inputs
        left = rule_push_down_selections(left, schema)
        right = rule_push_down_selections(right, schema)
        return Join(left, rel.cond, right)
    elif t == RelRef:
        return rel
    elif t == Select:
        inner = rule_push_down_selections(rel.inputs[0], schema)
        cond, res = push_down_selections(inner, rel.cond, schema)
        if cond is None:
            return res
        else:
            return Select(rel.cond, inner)


def get_rel(attr, schema):
    if attr.rel is not None:
        return attr.rel
    for r in schema:
        for a in schema[r]:
            if a == attr.name:
                return r
    return None


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


def get_attr_rel_for_eq(cond):
    return set([i.rel for i in cond.inputs if type(i) == AttrRef])


def get_attr_rels(cond, schema):
    return set([get_rel(i, schema) for i in cond.inputs if type(i) == AttrRef])


def get_rels(rel):
    if type(rel) == RelRef:
        return set([rel.rel])
    rel_list = [get_rels(i) for i in rel.inputs]
    if type(rel) == Rename:
        rel_list = [{rel.relname}]
    return set.union(*rel_list)


def rule_merge_selections(rel):
    t = type(rel)
    if t == Cross:
        left, right = rel.inputs
        left = rule_merge_selections(left)
        right = rule_merge_selections(right)
        return Cross(left, right)
    elif t == Project:
        target_rel = rule_merge_selections(rel.inputs[0])
        return Project(rel.attrs, target_rel)
    elif t == Rename:
        target_rel = rule_merge_selections(rel.inputs[0])
        return Rename(rel.relname, rel.attrnames, target_rel)
    elif t == Join:
        left, right = rel.inputs
        left = rule_merge_selections(left)
        right = rule_merge_selections(right)
        return Join(left, rel.cond, right)
    elif t == RelRef:
        return rel
    elif t == Select:
        # TODO: Test select in select
        cond = rel.cond
        target = rel.inputs[0]
        while type(target) == Select:
            cond = ValExprBinaryOp(cond, sym.AND, target.cond)
            target = target.inputs[0]
        target = rule_merge_selections(target)
        return Select(cond, target)


def rule_introduce_joins(rel):
    t = type(rel)
    if t == Cross:
        left, right = rel.inputs
        left = rule_introduce_joins(left)
        right = rule_introduce_joins(right)
        return Cross(left, right)
    elif t == Project:
        target_rel = rule_introduce_joins(rel.inputs[0])
        return Project(rel.attrs, target_rel)
    elif t == Rename:
        target_rel = rule_introduce_joins(rel.inputs[0])
        return Rename(rel.relname, rel.attrnames, target_rel)
    elif t == Join:
        left, right = rel.inputs
        left = rule_introduce_joins(left)
        right = rule_introduce_joins(right)
        return Join(left, rel.cond, right)
    elif t == RelRef:
        return rel
    elif t == Select:
        # TODO: Test select in select

        rels = get_rels(rel.inputs[0])
        if len(rels) < 2:
            return rel

        input_rel = rule_introduce_joins(rel.inputs[0])

        cond = rel.cond
        conds = []
        while cond.op is sym.AND:
            cond, right = cond.inputs
            conds.append(right)
        conds.append(cond)

        target_conds, left_conds = [], []

        for c in conds:
            attr_rels = get_attr_rel_for_eq(c)
            if len(attr_rels) == 2 and attr_rels <= rels and c.op == sym.EQ:
                target_conds.append(c)
            else:
                left_conds.append(c)

        target_cond = get_conjunctive_cond(target_conds)

        if target_cond is not None:
            target_cond, input_rel = introduce_join(target_cond, input_rel)

            left_cond = get_conjunctive_cond(left_conds)
            if left_cond is None:
                return input_rel

        return Select(rel.cond, input_rel)


def get_conjunctive_cond(conds):
    if len(conds) < 1:
        return None
    conds = list(reversed(conds))
    res = conds[0]
    for c in conds[1:]:
        res = ValExprBinaryOp(res, sym.AND, c)
    return res


def introduce_join(cond, rel):
    t = type(rel)
    if t == Cross:
        attr_rels = get_attr_rel_for_eq(cond)

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
