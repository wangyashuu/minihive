from sqlparse.sql import IdentifierList, Identifier, Where
from sqlparse.tokens import Keyword, Token, Whitespace, Newline

from radb.ast import (
    RelRef,
    Cross,
    Select,
    Project,
    ValExprBinaryOp,
    AttrRef,
    RAString,
    RANumber,
    Rename,
)
from radb.parse import RAParser as sym


def translate(tokens):
    select_target, from_target, where_target = None, None, None
    for t in remove_spaces(tokens):
        if select_target is None and (
            type(t) == Identifier
            or type(t) == IdentifierList
            or t.ttype == Token.Wildcard
        ):
            select_target = t
        elif from_target is None and (
            type(t) == Identifier or type(t) == IdentifierList
        ):
            from_target = t
        elif where_target is None and type(t) == Where:
            where_target = t

    obj = parse_from(from_target)
    if where_target:
        where_obj = parse_where(where_target)
        obj = Select(where_obj, obj)

    if select_target.ttype != Token.Wildcard:
        select_obj = parse_select(select_target)
        obj = Project(select_obj, obj)

    return obj


def remove_spaces(tokens):
    return [t for t in tokens if t.ttype != Whitespace and t.ttype != Newline]


def parse_from(token):
    if type(token) == IdentifierList:
        where_obj = None
        for t in token.get_identifiers():
            current_obj = parse_rel(t)
            where_obj = (
                current_obj if where_obj is None else Cross(where_obj, current_obj)
            )
        return where_obj
    else:
        return parse_rel(token)


def parse_rel(token):
    alias = token.get_alias()
    if alias is None:
        return RelRef(token.get_name())
    return Rename(alias, None, RelRef(token.get_real_name()))


def parse_select(token):
    # TODO attribute rename
    if type(token) == IdentifierList:
        return [parse_attr(t) for t in token.get_identifiers()]
    else:
        return [parse_attr(token)]


def parse_where(token):
    obj = None
    combination_op = None
    for t in remove_spaces(token.tokens[1:]):
        if t.ttype != Keyword:
            left = parse_var(t.left)
            right = parse_var(t.right)
            # token_iterator = meaningful_iterate(token.tokens)
            _, op_token = t.token_next(0, skip_ws=True, skip_cm=True)
            op = parse_comparison_op(op_token)
            current_obj = ValExprBinaryOp(left, op, right)
            obj = (
                current_obj
                if combination_op is None
                else ValExprBinaryOp(obj, combination_op, current_obj)
            )
        else:
            combination_op = parse_combination_op(t)
    return obj


def parse_var(token):
    if type(token) == Identifier:
        return parse_attr(token)
    return parse_literal(token)


def parse_literal(token):
    if token.ttype in [
        Token.Literal.String,
        Token.Literal.String.Single,
        Token.Literal.String.Symbol,
    ]:
        return RAString(token.value)
    elif token.ttype in [
        Token.Literal.Number,
        Token.Literal.Number.Float,
        Token.Literal.Number.Integer,
    ]:
        return RANumber(token.value)


def parse_attr(token):
    tokens = remove_spaces(token.tokens)
    rel = tokens[0].value if len(tokens) > 2 else None
    name = tokens[-1].value
    return AttrRef(rel, name)


def parse_comparison_op(token):
    comparison_ops = {
        "<": sym.LT,
        "<=": sym.LE,
        "=": sym.EQ,
        "!=": sym.NE,
        ">=": sym.GE,
        ">": sym.GT,
    }
    return comparison_ops[token.value]


def parse_combination_op(token):
    combination_ops = {"and": sym.AND, "or": sym.OR}
    return combination_ops[token.value.lower()]
