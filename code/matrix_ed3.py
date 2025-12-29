#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from scipy import sparse
import sys
import time
import traceback
import numpy as np
from pyspark import SparkContext, SparkConf

def parse_matrix_line(line):
    try:
        line = line.strip()
        if not line or line.startswith('#'): return None
        parts = line.split() if ' ' in line else (line.split(',') if ',' in line else line.split('\t'))
        if len(parts) != 3: return None
        return int(parts[0]), int(parts[1]), float(parts[2])
    except:
        return None

def load_matrix_rdd(sc, path, num_partitions):
    print("Loading matrix from: {}".format(path))
    return sc.textFile(path, minPartitions=num_partitions) \
             .map(parse_matrix_line) \
             .filter(lambda x: x is not None)

def naive_matrix_multiplication(sc, A_rdd, B_rdd, num_partitions):
    print(">>> Starting Naive Matrix Multiplication...")
    start_time = time.time()
    try:
        # Map to (k, (i, v_a)) and (k, (j, v_b))
        A_k = A_rdd.map(lambda x: (x[1], (x[0], x[2])))
        B_k = B_rdd.map(lambda x: (x[0], (x[1], x[2])))
        
        # Join on k, compute partial product, reduce by (i, j)
        C = A_k.join(B_k, numPartitions=num_partitions) \
               .map(lambda x: ((x[1][0][0], x[1][1][0]), x[1][0][1] * x[1][1][1])) \
               .reduceByKey(lambda x, y: x + y, numPartitions=num_partitions)
        
        count = C.count()
        return "naive", time.time() - start_time, count
    except:
        traceback.print_exc()
        return "naive", -1, 0

def broadcast_matrix_multiplication(sc, A_rdd, B_rdd, num_partitions):
    print(">>> Starting Broadcast Matrix Multiplication (CSR optimized)...")
    start_time = time.time()
    try:
        # Collect B to driver and build CSR matrix
        B_list = B_rdd.collect()
        if not B_list:
            return "broadcast_opt", 0, 0

        rows_b = [x[0] for x in B_list]
        cols_b = [x[1] for x in B_list]
        data_b = [x[2] for x in B_list]
        
        shape_b = (max(rows_b) + 1, max(cols_b) + 1)
        B_csr = sparse.csr_matrix((data_b, (rows_b, cols_b)), shape=shape_b)
        
        B_broadcast = sc.broadcast(B_csr)

        def map_join_optimized(part_data):
            local_B = B_broadcast.value
            chunk_A = list(part_data)
            if not chunk_A: return []
            
            rows_a = [x[0] for x in chunk_A]
            cols_a = [x[1] for x in chunk_A]
            data_a = [x[2] for x in chunk_A]
            
            max_r_a = max(rows_a)
            shape_a = (max_r_a + 1, local_B.shape[0])
            local_A = sparse.csr_matrix((data_a, (rows_a, cols_a)), shape=shape_a)
            
            # Vectorized multiplication using scipy
            local_C = local_A.dot(local_B).tocoo()
            
            # Yield non-zero elements
            return zip(zip(local_C.row, local_C.col), local_C.data)

        C = A_rdd.mapPartitions(map_join_optimized) \
                 .reduceByKey(lambda x, y: x + y, numPartitions=num_partitions)
        
        count = C.count()
        B_broadcast.unpersist()
        return "broadcast_csr", time.time() - start_time, count
        
    except Exception:
        traceback.print_exc()
        return "broadcast_csr", -1, 0

def block_matrix_multiplication(sc, A_rdd, B_rdd, num_partitions, block_size=1000):
    print(">>> Starting Block Matrix Multiplication (Block Size: {})".format(block_size))
    start_time = time.time()
    try:
        def to_block(x):
            r, c, v = x
            return ((int(r // block_size), int(c // block_size)), (int(r), int(c), float(v)))

        def collect_to_list(iterable):
            return list(iterable)

        # 1. Block construction
        A_blocks = A_rdd.map(to_block).groupByKey(numPartitions=num_partitions).mapValues(collect_to_list)
        B_blocks = B_rdd.map(to_block).groupByKey(numPartitions=num_partitions).mapValues(collect_to_list)

        # 2. Key alignment for join
        A_keyed = A_blocks.map(lambda x: (x[0][1], (x[0][0], x[1])))
        B_keyed = B_blocks.map(lambda x: (x[0][0], (x[0][1], x[1])))

        # 3. Block multiplication using sparse matrices
        def sparse_block_dot(joined_data):
            bk, (data_a, data_b) = joined_data
            bi, list_a = data_a
            bj, list_b = data_b
            
            def to_sparse_matrix(data_list, row_offset, col_offset):
                rows, cols, vals = [], [], []
                for r, c, v in data_list:
                    rows.append(r - row_offset)
                    cols.append(c - col_offset)
                    vals.append(v)
                return sparse.coo_matrix(
                    (vals, (rows, cols)), 
                    shape=(block_size, block_size)
                ).tocsr()

            mat_a = to_sparse_matrix(list_a, bi * block_size, bk * block_size)
            mat_b = to_sparse_matrix(list_b, bk * block_size, bj * block_size)
            
            res_mat = mat_a.dot(mat_b).tocoo()
            
            output = []
            row_origin = bi * block_size
            col_origin = bj * block_size
            
            for r, c, v in zip(res_mat.row, res_mat.col, res_mat.data):
                if v != 0:
                    output.append(((r + row_origin, c + col_origin), float(v)))
            return output

        C = A_keyed.join(B_keyed, numPartitions=num_partitions) \
                   .flatMap(sparse_block_dot) \
                   .reduceByKey(lambda x, y: x + y, numPartitions=num_partitions)
                   
        count = C.count()
        return "block_sparse", time.time() - start_time, count

    except Exception:
        print("Error in block_matrix_multiplication:")
        traceback.print_exc()
        return "block_sparse", -1, 0

def main():
    if len(sys.argv) != 6:
        print("Usage: spark-submit script.py <scenario> <A_path> <B_path> <out_file> <method>")
        sys.exit(1)

    scenario = sys.argv[1]
    A_path = sys.argv[2]
    B_path = sys.argv[3]
    out_file = sys.argv[4]
    method_requested = sys.argv[5]

    app_name = "Matrix_{}_{}".format(scenario, method_requested)
    conf = SparkConf().setAppName(app_name)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    sc = SparkContext(conf=conf)
    parallelism = int(sc.getConf().get("spark.default.parallelism", "100"))

    rdd_A = load_matrix_rdd(sc, A_path, parallelism).cache()
    rdd_B = load_matrix_rdd(sc, B_path, parallelism).cache()
    
    # Materialize cache
    rdd_A.count()
    rdd_B.count()

    if method_requested == "naive":
        name, dur, count = naive_matrix_multiplication(sc, rdd_A, rdd_B, parallelism)
    elif method_requested == "broadcast":
        name, dur, count = broadcast_matrix_multiplication(sc, rdd_A, rdd_B, parallelism)
    elif method_requested == "block":
        name, dur, count = block_matrix_multiplication(sc, rdd_A, rdd_B, parallelism)
    else:
        name, dur, count = "unknown", 0, 0

    res_line = "{},{},{:.4f},{}\n".format(scenario, name, dur, count)
    with open(out_file, 'a') as f:
        f.write(res_line)
    
    print("\nExperiment Finished: {}".format(res_line.strip()))
    
    rdd_A.unpersist()
    rdd_B.unpersist()
    sc.stop()

if __name__ == "__main__":
    main()