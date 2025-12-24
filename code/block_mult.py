# -*- coding: utf-8 -*-
from __future__ import print_function # 确保 print 兼容性
import sys
import time
import numpy as np
from pyspark import SparkContext, SparkConf

# ================= 配置区域 =================
BLOCK_SIZE = 1000 
# ===========================================

def parse_entry(line):
    parts = line.split(',')
    return int(parts[0]), int(parts[1]), float(parts[2])

def map_to_block(entry):
    r, c, v = entry
    br = r // BLOCK_SIZE
    bc = c // BLOCK_SIZE
    lr = r % BLOCK_SIZE
    lc = c % BLOCK_SIZE
    return ((br, bc), (lr, lc, v))

def create_local_matrix(iterator):
    data_list = list(iterator)
    if not data_list:
        return []
    block_key = data_list[0][0]
    mat = np.zeros((BLOCK_SIZE, BLOCK_SIZE))
    for _, (lr, lc, v) in data_list:
        mat[lr, lc] = v
    return [(block_key, mat)]

def block_multiply(item):
    """
    执行块乘法
    item结构: (k, (Iterable_of_A_Blocks, Iterable_of_B_Blocks))
    """
    k_index, (iterable_A, iterable_B) = item
    
    list_A = list(iterable_A)
    list_B = list(iterable_B)
    
    results = []
    
    # 修正后的解包逻辑
    for (row_block_idx, mat_A) in list_A:
        for (col_block_idx, mat_B) in list_B:
            # 核心计算：NumPy 矩阵乘法
            res_mat = np.dot(mat_A, mat_B)
            
            if np.any(res_mat):
                results.append(((row_block_idx, col_block_idx), res_mat))
                
    return results

def sum_matrices(m1, m2):
    return m1 + m2

def main(input_a, input_b, output_path):
    conf = SparkConf().setAppName("BlockMatrixMult_Numpy")
    sc = SparkContext(conf=conf)

    start_time = time.time()

    # 1. 读取
    raw_A = sc.textFile(input_a).map(parse_entry).filter(lambda x: x[2] != 0)
    raw_B = sc.textFile(input_b).map(parse_entry).filter(lambda x: x[2] != 0)

    # 2. 构建块
    blocks_A = raw_A.map(map_to_block).groupByKey().mapValues(lambda vals: create_local_matrix([((0,0), v) for v in vals])[0][1])
    blocks_B = raw_B.map(map_to_block).groupByKey().mapValues(lambda vals: create_local_matrix([((0,0), v) for v in vals])[0][1])

    # 3. 调整 Key 为 k (Join key)
    A_by_k = blocks_A.map(lambda x: (x[0][1], (x[0][0], x[1])))
    B_by_k = blocks_B.map(lambda x: (x[0][0], (x[0][1], x[1])))

    # 4. Shuffle (CoGroup)
    joined = A_by_k.cogroup(B_by_k)

    # 5. 计算
    partial_products = joined.flatMap(block_multiply)

    # 6. 聚合
    final_result = partial_products.reduceByKey(sum_matrices)

    # 7. 触发 Action
    count = final_result.count()
    
    end_time = time.time()
    
    # === 修改处：使用函数式打印，更安全 ===
    print("### Block-Opt Execution Time: {:.4f} seconds".format(end_time - start_time))
    print("### Result Blocks Count: {}".format(count))
    sc.stop()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
