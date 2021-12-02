from radb.ast import Cross, Project, Rename, Join, RelRef, Select
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
        cond = rel.cond
        obj = rel.inputs[0]
        while cond.op is sym.AND:
            left, right = cond.inputs
            obj = Select(right, obj)
            cond = left
        return Select(cond, obj)


def rule_push_down_selections(ra, dd):
    pass


def rule_merge_selections(ra):
    pass


def rule_introduce_joins(ra):
    pass
