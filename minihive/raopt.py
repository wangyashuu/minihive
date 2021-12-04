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
        elif attr_rels < rels:
            cond, potential = push_down_selections(rel.inputs[0], cond, schema)
            if cond is None:
                return None, Select(rel.cond, potential)
    return cond, rel


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


def rule_introduce_joins(ra):
    pass
