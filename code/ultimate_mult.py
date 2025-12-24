# ============================================================
# Ultimate Spark Distributed Matrix Multiplication
# Author: (Your Name)
# Description:
#   - Sparse-aware
#   - Block Matrix Multiply
#   - Broadcast Optimization
#   - Partition / Cache Optimized
# ============================================================

from pyspark import SparkConf, SparkContext
from operator import add
from collections import defaultdict
import random
import sys

# ------------------------------------------------------------
# Spark Context
# ------------------------------------------------------------
conf = (
    SparkConf()
    .setAppName("UltimateSparkMatrixMultiply")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
)
sc = SparkContext.getOrCreate(conf)

# ------------------------------------------------------------
# Matrix Generators
# ------------------------------------------------------------
def generate_sparse_matrix(m, n, density, seed=42):
    """
    Generate sparse matrix in COO format
    (i, j, value)
    """
    random.seed(seed)
    data = [
        (i, j, random.random())
        for i in range(m)
        for j in range(n)
        if random.random() < density
    ]
    return sc.parallelize(data)


def generate_dense_matrix(m, n):
    """
    Generate dense matrix (慎用)
    """
    data = [(i, j, random.random()) for i in range(m) for j in range(n)]
    return sc.parallelize(data)


# ------------------------------------------------------------
# Block Utilities
# ------------------------------------------------------------
def to_block(rdd, block_size):
    """
    (i, j, v) -> ((iB, jB), (i, j, v))
    """
    return rdd.map(
        lambda x: ((x[0] // block_size, x[1] // block_size), x)
    )


def local_block_multiply(A_block, B_block):
    """
    Perform local sparse block multiplication
    A_block: [(i, k, val)]
    B_block: [(k, j, val)]
    return: iterator[((i, j), val)]
    """
    A_map = defaultdict(list)
    for i, k, v in A_block:
        A_map[k].append((i, v))

    result = defaultdict(float)
    for k, j, v_b in B_block:
        for i, v_a in A_map.get(k, []):
            result[(i, j)] += v_a * v_b

    return result.items()


# ------------------------------------------------------------
# Ultimate Matrix Multiplication
# ------------------------------------------------------------
def ultimate_matmul(
    A,
    B,
    block_size=256,
    broadcast_threshold=5_000_000,
    persist_result=True
):
    """
    A, B: RDD[(i, j, value)]
    """

    # --------------------------------------------------------
    # Strategy 1: Broadcast Small Matrix
    # --------------------------------------------------------
    B_nnz = B.count()

    if B_nnz < broadcast_threshold:
        print("[INFO] Using BROADCAST strategy")

        B_local = sc.broadcast(B.collect())

        C = (
            A.flatMap(
                lambda x: [
                    ((x[0], b[1]), x[2] * b[2])
                    for b in B_local.value
                    if b[0] == x[1]
                ]
            )
            .reduceByKey(add)
        )

        if persist_result:
            C = C.persist()

        return C

    # --------------------------------------------------------
    # Strategy 2: Block Matrix Multiply
    # --------------------------------------------------------
    print("[INFO] Using BLOCK strategy")

    A_block = to_block(A, block_size)
    B_block = to_block(B, block_size)

    # Tag blocks
    A_keyed = A_block.map(
        lambda x: (x[0][1], ('A', x[0][0], x[1]))
    )
    B_keyed = B_block.map(
        lambda x: (x[0][0], ('B', x[0][1], x[1]))
    )

    num_partitions = max(
        A.getNumPartitions(),
        B.getNumPartitions()
    )

    grouped = (
        A_keyed.union(B_keyed)
        .partitionBy(num_partitions)
        .groupByKey()
    )

    def compute_blocks(kv):
        _, values = kv
        A_blocks = defaultdict(list)
        B_blocks = defaultdict(list)

        for tag, block_id, elem in values:
            if tag == 'A':
                A_blocks[block_id].append(elem)
            else:
                B_blocks[block_id].append(elem)

        for iB in A_blocks:
            for jB in B_blocks:
                for (i, j), v in local_block_multiply(
                    A_blocks[iB],
                    B_blocks[jB]
                ):
                    yield ((i, j), v)

    C = grouped.flatMap(compute_blocks).reduceByKey(add)

    if persist_result:
        C = C.persist()

    return C


# ------------------------------------------------------------
# Main (Example Run)
# ------------------------------------------------------------
if __name__ == "__main__":

    # Example parameters
    M = 10000
    K = 10000
    N = 10000
    DENSITY = 0.01
    BLOCK_SIZE = 256

    print("[INFO] Generating matrices...")
    A = generate_sparse_matrix(M, K, DENSITY)
    B = generate_sparse_matrix(K, N, DENSITY)

    print("[INFO] Starting matrix multiplication...")
    C = ultimate_matmul(
        A,
        B,
        block_size=BLOCK_SIZE,
        broadcast_threshold=2_000_000
    )

    print("[INFO] Sample output:")
    print(C.take(10))

    print("[INFO] Done.")
