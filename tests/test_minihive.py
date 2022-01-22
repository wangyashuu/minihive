from itertools import count
import pytest
import minihive.ra2mr as ra2mr
from minihive.minihive import eval, clear_local_tmpfiles, dd
import minihive.costcounter as costcounter
import time


def get_query_counts(query, optimize=False):
    clear_local_tmpfiles()
    output = eval(
        sf=0.1, dd=dd, env=ra2mr.ExecEnv.LOCAL, query=query, optimize=optimize
    )

    lines = output.readlines()
    n_lines = len(lines)
    output.close()
    hdfs_counts = costcounter.compute_hdfs_costs()
    return n_lines, hdfs_counts


def get_counts(query):
    counts = (1, 0)
    tic = time.perf_counter()
    counts = get_query_counts(query, optimize=False)
    toc = time.perf_counter()
    diff = toc - tic
    print("time diff: ", diff, ", counts: ", counts)
    tic = time.perf_counter()
    opt_counts = get_query_counts(query, optimize=True)
    toc = time.perf_counter()
    opt_diff = toc - tic
    print("time diff: ", diff, ", counts: ", counts)
    print("time diff: ", opt_diff, ", counts_opt: ", opt_counts)
    return counts + opt_counts  # opt_counts


def test_query_1():
    query = (
        "select distinct C_NAME, C_ADDRESS from CUSTOMER where C_CUSTKEY=42"
    )
    n_lines, hdfs_counts, n_lines_opt, hdfs_counts_opt = get_counts(query)
    assert n_lines_opt == n_lines  # 1
    assert hdfs_counts_opt < hdfs_counts / 3  # 448 -> 77


def test_query_2():
    query = (
        "select distinct C.C_NAME, C.C_ADDRESS from CUSTOMER C where"
        " C.C_NATIONKEY=7"
    )
    n_lines, hdfs_counts, n_lines_opt, hdfs_counts_opt = get_counts(query)
    assert n_lines_opt == n_lines  # 57
    assert hdfs_counts_opt < hdfs_counts / 3  #  467940 -> 4392


def test_query_3():
    query = (
        "select distinct * from CUSTOMER, NATION where"
        " CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and"
        " NATION.N_NAME='GERMANY'"
    )
    n_lines, hdfs_counts, n_lines_opt, hdfs_counts_opt = get_counts(query)
    assert n_lines_opt == n_lines  # 57
    assert hdfs_counts_opt < hdfs_counts  # 29683 -> 29523


def test_query_4():
    query = (
        "select distinct CUSTOMER.C_CUSTKEY from CUSTOMER, NATION where"
        " CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and"
        " NATION.N_NAME='GERMANY'"
    )
    n_lines, hdfs_counts, n_lines_opt, hdfs_counts_opt = get_counts(query)
    assert n_lines_opt == n_lines  # 57
    assert hdfs_counts_opt < hdfs_counts  # 31240 -> 31080


def test_query_5():
    query = (
        "select distinct CUSTOMER.C_CUSTKEY from CUSTOMER, NATION where"
        " CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and"
        " CUSTOMER.C_CUSTKEY=42"
    )
    n_lines, hdfs_counts, n_lines_opt, hdfs_counts_opt = get_counts(query)
    assert n_lines_opt == n_lines  # 1
    assert hdfs_counts_opt < hdfs_counts  # 902 -> 531


def test_query_6():
    query = (  # 239,  416162 -> 160309
        "select distinct CUSTOMER.C_CUSTKEY from CUSTOMER, NATION, REGION"
        " where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_REGIONKEY"
        " = REGION.R_REGIONKEY"
    )
    n_lines, hdfs_counts, n_lines_opt, hdfs_counts_opt = get_counts(query)
    assert n_lines_opt == n_lines  # 1500
    assert hdfs_counts_opt < hdfs_counts  # 1809028 -> 1061077


def test_query_7():
    query = (
        "select distinct CUSTOMER.C_CUSTKEY from REGION, NATION, CUSTOMER"
        " where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and"
        " NATION.N_REGIONKEY = REGION.R_REGIONKEY"
    )
    n_lines, hdfs_counts, n_lines_opt, hdfs_counts_opt = get_counts(query)
    assert n_lines_opt == n_lines  # 1500
    assert hdfs_counts_opt <= hdfs_counts  # 1061077 -> 1061077


def test_query_8():
    query = (
        "select distinct * from ORDERS, CUSTOMER where"
        " ORDERS.O_ORDERPRIORITY='1-URGENT' and"
        " CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY"
    )
    n_lines, hdfs_counts, n_lines_opt, hdfs_counts_opt = get_counts(query)
    assert n_lines_opt == n_lines  # 3020
    assert hdfs_counts_opt < hdfs_counts  # 3055418 -> 2062404


def test_query_9():
    query = (
        "select distinct * from CUSTOMER,ORDERS,LINEITEM where"
        " CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY and ORDERS.O_ORDERKEY ="
        " LINEITEM.L_ORDERKEY and LINEITEM.L_SHIPMODE='AIR' and"
        " CUSTOMER.C_MKTSEGMENT = 'HOUSEHOLD'"
    )
    n_lines, hdfs_counts, n_lines_opt, hdfs_counts_opt = get_counts(query)
    assert n_lines_opt == n_lines  # 1659
    assert hdfs_counts_opt < hdfs_counts  # 8513358 -> 3911209


def test_query_10():
    query = (
        "select distinct * from LINEITEM,ORDERS,CUSTOMER where"
        " CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY and ORDERS.O_ORDERKEY ="
        " LINEITEM.L_ORDERKEY and LINEITEM.L_SHIPMODE='AIR' and"
        " CUSTOMER.C_MKTSEGMENT = 'HOUSEHOLD'"
    )
    n_lines, hdfs_counts, n_lines_opt, hdfs_counts_opt = get_counts(query)
    assert n_lines_opt == n_lines  # 1659
    assert hdfs_counts_opt < hdfs_counts / 3  # 13912617 -> 3911209
