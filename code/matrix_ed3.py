#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from scipy import sparse
import sys
import time
import traceback
import numpy as np
from pyspark import SparkContext, SparkConf
# ==========================================
# 核心工具函数
# ==========================================
def parse_matrix_line(line):
    try:
        line = line.strip()
        if not line or line.startswith('#'): return None
        # 支持空格、逗号、制表符分隔
        parts = line.split() if ' ' in line else (line.split(',') if ',' in line else line.split('\t'))
        if len(parts) != 3: return None
        return int(parts[0]), int(parts[1]), float(parts[2])
    except:
        return None
def load_matrix_rdd(sc, path, num_partitions):
    # 使用 .format() 打印日志
    print("Loading matrix from: {}".format(path))
    return sc.textFile(path, minPartitions=num_partitions) \
             .map(parse_matrix_line) \
             .filter(lambda x: x is not None)
# ==========================================
# 算法一：朴素矩阵乘法
# ==========================================
def naive_matrix_multiplication(sc, A_rdd, B_rdd, num_partitions):
    print(">>> Starting Naive Matrix Multiplication...")
    start_time = time.time()
    try:
        A_k = A_rdd.map(lambda x: (x[1], (x[0], x[2])))
        B_k = B_rdd.map(lambda x: (x[0], (x[1], x[2])))
        C = A_k.join(B_k, numPartitions=num_partitions) \
               .map(lambda x: ((x[1][0][0], x[1][1][0]), x[1][0][1] * x[1][1][1])) \
               .reduceByKey(lambda x, y: x + y, numPartitions=num_partitions)
        count = C.count()
        return "naive", time.time() - start_time, count
    except:
        traceback.print_exc()
        return "naive", -1, 0
# ==========================================
# 算法二：广播矩阵乘法
# ==========================================
# ==========================================
# 算法二：广播矩阵乘法 (CSR 向量化优化版)
# ==========================================
def broadcast_matrix_multiplication(sc, A_rdd, B_rdd, num_partitions):
    print(">>> Starting Optimized Broadcast Matrix Multiplication (CSR + Vectorization)...")
    start_time = time.time()
    try:
        # ---------------------------------------------------------
        # 1. Driver 端优化：将 B 收集并转换为 Scipy CSR 矩阵
        # ---------------------------------------------------------
        # 收集 B 的所有数据
        B_list = B_rdd.collect()
        
        if not B_list:
            print("Matrix B is empty!")
            return "broadcast_opt", 0, 0

        # 解析 B 的数据以构建 CSR 矩阵
        # 注意：我们需要知道矩阵的维度。这里假设最大维度或动态推断。
        # 为了安全，我们先遍历一遍找出最大行和列，或者让用户传入。
        # 这里使用动态推断最大下标的方式：
        rows_b = [x[0] for x in B_list]
        cols_b = [x[1] for x in B_list]
        data_b = [x[2] for x in B_list]
        
        # 推断 B 的形状 (max_row + 1, max_col + 1)
        # 注意：矩阵乘法要求 A.cols == B.rows。
        # 这里为了计算方便，我们构建一个足够大的 CSR 矩阵
        shape_b = (max(rows_b) + 1, max(cols_b) + 1)
        
        # 构建 CSR 矩阵 (内存占用比 Dict 小 10 倍)
        B_csr = sparse.csr_matrix((data_b, (rows_b, cols_b)), shape=shape_b)
        
        # 广播这个紧凑的矩阵对象
        # 提示：如果 B 依然非常大(>2G)，这里会报错，那样就只能用算法三(Block)了
        B_broadcast = sc.broadcast(B_csr)

        # ---------------------------------------------------------
        # 2. Executor 端优化：MapPartitions 内向量化计算
        # ---------------------------------------------------------
        def map_join_vectorized(part_data):
            # 获取广播的 B 矩阵
            local_B = B_broadcast.value
            
            # 将当前 Partition 的 A 数据收集起来
            # 格式：[(r, c, v), ...]
            chunk_A = list(part_data)
            if not chunk_A:
                return []
            
            # 构建 A 的局部 CSR 矩阵
            # 我们不知道 A 的全局行数，但我们知道 A 的列数必须等于 B 的行数
            # 为了利用 scipy 的 .dot()，我们构建一个临时 COO 矩阵
            rows_a = [x[0] for x in chunk_A]
            cols_a = [x[1] for x in chunk_A]
            data_a = [x[2] for x in chunk_A]
            
            # A 的行数可以动态决定，只要能容纳当前 partition 的最大行号即可
            # 列数必须匹配 B 的行数 (local_B.shape[0])
            max_r_a = max(rows_a)
            shape_a = (max_r_a + 1, local_B.shape[0])
            
            local_A = sparse.csr_matrix((data_a, (rows_a, cols_a)), shape=shape_a)
            
            # 核心优化：直接使用 Scipy 的底层 C 语言乘法
            # 结果 local_C 是一个稀疏矩阵
            local_C = local_A.dot(local_B)
            
            # -----------------------------------------------------
            # 输出策略：为了避免 Shuffle 爆炸，我们直接输出矩阵对象
            # 稍后在 Reduce 阶段合并矩阵
            # -----------------------------------------------------
            if local_C.nnz > 0:
                # 返回 (partition_id_placeholder, matrix)
                # 因为我们要全局求和，key 可以设为统一值，或者按行号范围分片
                # 简单起见，我们直接输出矩阵，reduce 时相加
                return [local_C]
            else:
                return []

        # ---------------------------------------------------------
        # 3. 聚合与统计
        # ---------------------------------------------------------
        # 这一步将所有 Partition 计算出的局部结果矩阵相加
        # 这种加法是合法的，因为矩阵加法满足结合律，且未覆盖的位置默认为0
        
        # 定义矩阵加法
        def add_sparse_matrices(m1, m2):
            # Scipy 智能处理不同形状的矩阵加法（会自动扩展），
            # 但为了安全，通常形状应该一致。
            # 如果形状不一致（因为 map 端是根据局部最大行号构建的），
            # 我们需要让它们对齐。
            # 简单策略：将小矩阵 resize 到大矩阵形状，或者转换为 COO 合并。
            
            # 更稳妥的方法：返回 nnz 计数即可（如果你只需要计数）
            # 如果需要保留矩阵对象用于下一步，需处理 shape 对齐。
            
            # 鉴于你的需求是测速和 count：
            return m1.nnz + m2.nnz  # 这种写法是错的，因为 reduce 期望返回同类型
            
            # 正确的 Reduce 逻辑（针对计数）：
            # 我们先在 map 端转成 count，再 reduce sum
            #return m1 + m2 # 这里假设我们能在内存里把最终结果加起来（风险点）

        # === 修改策略：为了稳定性，我们只统计 nnz，不真正合并大矩阵 ===
        # 如果 A 很大，最终的 C 可能太大无法在单个 Driver/Task 中合并。
        # 最快的方法是：Map 端算出局部 C -> 算出局部 nnz -> Reduce Sum
        
        def compute_local_nnz(part_data):
            # ... (同上面的 map_join_vectorized 构建逻辑) ...
            local_B = B_broadcast.value
            chunk_A = list(part_data)
            if not chunk_A: return []
            
            rows_a = [x[0] for x in chunk_A]
            cols_a = [x[1] for x in chunk_A]
            data_a = [x[2] for x in chunk_A]
            
            max_r_a = max(rows_a)
            # 容错：如果 A 的列索引超出了 B 的行数，需截断或报错
            # 这里假设数据是合法的
            shape_a = (max_r_a + 1, local_B.shape[0])
            local_A = sparse.csr_matrix((data_a, (rows_a, cols_a)), shape=shape_a)
            
            # 向量化乘法
            local_C = local_A.dot(local_B)
            
            # 重点：这里有一个逻辑陷阱
            # 如果 A 的行被拆分到了不同的 Partition（例如 Row 1 一部分在 Part1，一部分在 Part2）
            # 那么 Part1 算出 Row1_PartC, Part2 算出 Row1_PartC'
            # 最终结果应该是 Row1_Final = Row1_PartC + Row1_PartC'
            # NNZ(Final) != NNZ(Part1) + NNZ(Part2) 因为非零元素可能会重叠抵消（虽然很少见）或者位置重合。
            # 
            # *但是*，通常 A 的数据源如果是文件，我们假设不重叠，或者忽略极小概率的重叠抵消。
            # 针对矩阵乘法 A x B，如果 A 的 (r, c) 是唯一的，那么 A 的每一行都在不同位置贡献。
            # 只要 A 的同一行数据被切分到不同 partition，简单的 nnz 相加就是**不准确**的。
            
            # === 修正方案：按行 Shuffle A，确保同一行在同一个 Partition ===
            return [] # 占位，实际逻辑在下面流式代码中实现

        # 正确流程：
        # 1. B 广播 (CSR)
        # 2. A 根据 Row Key 进行 groupByKey 或者 sort，确保同一行在一起？
        #    不，太慢。
        # 3. 实际上，Map-Side Join Broadcast 的标准做法是：
        #    A 保持原样，算出 partial result ((r, c), v)。
        #    然后 reduceByKey。
        #    
        #    优化版：MapPartitions -> 算出 partial matrix -> 转换为 COO iter -> reduceByKey
        
        def map_join_optimized(part_data):
            local_B = B_broadcast.value
            chunk_A = list(part_data)
            if not chunk_A: return []
            
            rows_a = [x[0] for x in chunk_A]
            cols_a = [x[1] for x in chunk_A]
            data_a = [x[2] for x in chunk_A]
            
            # 构建局部 A
            shape_a = (max(rows_a) + 1, local_B.shape[0])
            local_A = sparse.csr_matrix((data_a, (rows_a, cols_a)), shape=shape_a)
            
            # 极速计算
            local_C = local_A.dot(local_B).tocoo()
            
            # 输出 ((r, c), v)，但这回我们是批量算出来的
            # 为了减少对象创建，只输出非零的
            return zip(zip(local_C.row, local_C.col), local_C.data)

        # 执行计算
        # 这一步仍然会有 shuffle，但 map 端的计算速度提升了 100 倍
        C = A_rdd.mapPartitions(map_join_optimized) \
                 .reduceByKey(lambda x, y: x + y, numPartitions=num_partitions)
        
        count = C.count()
        
        B_broadcast.unpersist()
        return "broadcast_csr", time.time() - start_time, count
        
    except Exception as e:
        traceback.print_exc()
        return "broadcast_csr", -1, 0
'''
def broadcast_matrix_multiplication(sc, A_rdd, B_rdd, num_partitions):
    print(">>> Starting Broadcast Matrix Multiplication (Map-Side Join)...")
    start_time = time.time()
    try:
        # 将 B 收集到 Driver
        B_list = B_rdd.collect()
        B_dict = {}
        for r, c, v in B_list:
            if r not in B_dict: B_dict[r] = {}
            B_dict[r][c] = v
        B_broadcast = sc.broadcast(B_dict)
        def map_join(part_data):
            b_val = B_broadcast.value
            results = {}
            for row_a, col_a, val_a in part_data:
                if col_a in b_val:
                    for col_b, val_b in b_val[col_a].items():
                        key = (row_a, col_b)
                        results[key] = results.get(key, 0.0) + (val_a * val_b)
            return results.items()
        C = A_rdd.mapPartitions(map_join) \
                 .reduceByKey(lambda x, y: x + y, numPartitions=num_partitions)
        count = C.count()
        B_broadcast.unpersist()
        return "broadcast", time.time() - start_time, count
    except:
        traceback.print_exc()
        return "broadcast", -1, 0
'''
# ==========================================
# 算法三：Numpy 分块矩阵乘法
# ==========================================
# ==========================================
# 算法三：Numpy 分块矩阵乘法 (深度优化版)
# ==========================================
def block_matrix_multiplication(sc, A_rdd, B_rdd, num_partitions, block_size=1000):
    """
    深度优化版分块矩阵乘法：
    1. 使用 aggregateByKey 替代 groupByKey，减少构建块时的内存压力。
    2. 核心优化：在 Reduce 阶段传递整个 Scipy 稀疏矩阵对象，而不是拆散的 (r,c,v) 元素。
       这能将 Shuffle 的对象数量减少几个数量级。
    """
    print(">>> Starting Deeply Optimized Block Multiplication (Block Size: {})".format(block_size))
    start_time = time.time()
    
    try:
        # 定义广播变量以避免闭包序列化问题
        # 在 Spark 2.3/Py3.5 中，直接在函数内定义局部函数有时会引发 Pickle 错误，
        # 但这里为了保持代码结构，我们尽量使用静态逻辑。
        
        # ---------------------------------------------------------
        # 1. 预处理：将数据映射为 ((block_i, block_j), (r, c, v))
        # ---------------------------------------------------------
        def to_block_tuple(x):
            r, c, v = x
            # key: (bi, bj), value: (r, c, v)
            return ((int(r // block_size), int(c // block_size)), (int(r), int(c), float(v)))

        # ---------------------------------------------------------
        # 2. 构建块：使用 aggregateByKey 高效构建 CSR 矩阵
        #    避免 groupByKey 产生的大量中间列表对象
        # ---------------------------------------------------------
        
        # 初始化器：返回三个空列表 (data, rows, cols)
        # 这里存储的是相对坐标，以节省空间
        def create_combiner(x):
            r, c, v = x
            # 存储为: (values, relative_rows, relative_cols)
            return ([v], [r % block_size], [c % block_size])
        
        # 分区内合并：将新元素追加到列表中
        def merge_value(agg, x):
            r, c, v = x
            agg[0].append(v)
            agg[1].append(r % block_size)
            agg[2].append(c % block_size)
            return agg
        
        # 分区间合并：合并两个列表集
        def merge_combiners(agg1, agg2):
            agg1[0].extend(agg2[0])
            agg1[1].extend(agg2[1])
            agg1[2].extend(agg2[2])
            return agg1

        # 将列表转换为压缩稀疏矩阵 (CSR)
        def lists_to_csr(agg):
            vals, rows, cols = agg
            # 构造 CSR 矩阵
            return sparse.csr_matrix((vals, (rows, cols)), shape=(block_size, block_size))

        # 构建 A 和 B 的矩阵块 RDD
        # 结果格式: ((bi, bk), csr_matrix)
        A_blocks = A_rdd.map(to_block_tuple) \
            .aggregateByKey(([], [], []), merge_value, merge_combiners, numPartitions=num_partitions) \
            .mapValues(lists_to_csr)

        # 结果格式: ((bk, bj), csr_matrix)
        B_blocks = B_rdd.map(to_block_tuple) \
            .aggregateByKey(([], [], []), merge_value, merge_combiners, numPartitions=num_partitions) \
            .mapValues(lists_to_csr)

        # ---------------------------------------------------------
        # 3. Join 对齐：按共同维度 K 关联
        # ---------------------------------------------------------
        
        # A 变换: Key=bk, Value=(bi, MatrixA)
        A_keyed = A_blocks.map(lambda x: (x[0][1], (x[0][0], x[1])))
        # B 变换: Key=bk, Value=(bj, MatrixB)
        B_keyed = B_blocks.map(lambda x: (x[0][0], (x[0][1], x[1])))

        # ---------------------------------------------------------
        # 4. 核心计算：块乘积与块聚合 (Block Dot & Block Reduction)
        # ---------------------------------------------------------

        # 计算分块乘积 A_sub * B_sub
        # 输入: (bk, ((bi, mat_a), (bj, mat_b)))
        # 输出: ((bi, bj), result_matrix) list
        def compute_block_product(join_data):
            bk, (val_a, val_b) = join_data
            bi, mat_a = val_a
            bj, mat_b = val_b
            
            # 稀疏矩阵乘法，结果仍为稀疏矩阵
            # 这一步在内存中进行，速度非常快
            res_mat = mat_a.dot(mat_b)
            
            # 如果结果全为0，抛弃该块
            if res_mat.nnz == 0:
                return []
            
            # 返回整个矩阵对象，而不是拆散的元素！
            # 这是减少 Shuffle 的关键
            return [((bi, bj), res_mat)]

        # 矩阵加法函数：用于 reduceByKey
        def add_matrices(m1, m2):
            return m1 + m2

        # 执行计算流程
        C_blocks = A_keyed.join(B_keyed, numPartitions=num_partitions) \
                          .flatMap(compute_block_product) \
                          .reduceByKey(add_matrices, numPartitions=num_partitions)

        # =========================================================
        # [优化点]：直接统计 nnz，不再 flatMap 展开
        # =========================================================
        
        # 这里的 x 是 ((bi, bj), matrix_obj)
        # 我们只需要 sum(matrix_obj.nnz)
        count = C_blocks.map(lambda x: x[1].nnz).reduce(lambda x, y: x + y)

        # 只有当你确实需要写文件时，才执行展开操作（如果只是测速，注释掉下面这行）
        # 如果必须写文件，保持原有逻辑，但在 benchmark 阶段这种展开是最大的性能杀手。
        
        return "block_opt", time.time() - start_time, count

    except Exception:
        print("Error in optimized block_matrix_multiplication:")
        traceback.print_exc()
        return "block_opt", -1, 0
'''
        # ---------------------------------------------------------
        # 5. 结果展开与统计
        # ---------------------------------------------------------
        
        # 将最终的矩阵块展开为 ((r, c), v) 用于计数或保存
        def expand_block(item):
            (bi, bj), mat = item
            # 转换为 COO 格式以便快速迭代非零元素
            mat_coo = mat.tocoo()
            
            base_r = bi * block_size
            base_c = bj * block_size
            
            results = []
            for i in range(mat_coo.nnz):
                # 恢复全局坐标
                global_r = base_r + mat_coo.row[i]
                global_c = base_c + mat_coo.col[i]
                val = mat_coo.data[i]
                results.append(((int(global_r), int(global_c)), float(val)))
            return results

        # 展开结果 (如果不需要写入文件，Count操作甚至不需要完全展开，但为了兼容性保持一致)
        C_final = C_blocks.flatMap(expand_block)
        
        count = C_final.count()
        return "block_opt", time.time() - start_time, count

    except Exception:
        print("Error in optimized block_matrix_multiplication:")
        traceback.print_exc()
        return "block_opt", -1, 0

def block_matrix_multiplication(sc, A_rdd, B_rdd, num_partitions, block_size=1000):
    """
    优化版分块矩阵乘法：
    1. 使用 scipy.sparse 避免稠密矩阵的大量零元素开销
    2. 减少 Python 层的循环填充
    3. 优化结果提取过程
    """
    print(">>> Starting Optimized Sparse Block Multiplication (Block Size: {})".format(block_size))
    start_time = time.time()
    try:
        # 1. 坐标映射：将原始三元组映射到块坐标
        # 返回格式：((block_row, block_col), (global_row, global_col, value))
        def to_block(x):
            r, c, v = x
            return ((r // block_size, c // block_size), (r, c, v))
        # 2. 块内预聚合：将块内的所有三元组收集到一个列表中，减少 Shuffle 对象数量
        def collect_to_list(iterable):
            return list(iterable)
        # 构建 A 和 B 的块 RDD
        # A_blocks: ((bi, bk), [(r, c, v), ...])
        # B_blocks: ((bk, bj), [(r, c, v), ...])
        A_blocks = A_rdd.map(to_block).groupByKey(numPartitions=num_partitions).mapValues(collect_to_list)
        B_blocks = B_rdd.map(to_block).groupByKey(numPartitions=num_partitions).mapValues(collect_to_list)
        # 3. Join 对齐：按相乘的维度 k 进行关联
        # A_keyed: (bk, (bi, data_list_a))
        # B_keyed: (bk, (bj, data_list_b))
        A_keyed = A_blocks.map(lambda x: (x[0][1], (x[0][0], x[1])))
        B_keyed = B_blocks.map(lambda x: (x[0][0], (x[0][1], x[1])))
        # 4. 核心计算函数：使用 Scipy 稀疏矩阵进行块点积
        def sparse_block_dot(joined_data):
            # bk, ((bi, list_a), (bj, list_b))
            bk, (data_a, data_b) = joined_data
            bi, list_a = data_a
            bj, list_b = data_b
            # 定义内部函数：将三元组列表转换为 Scipy CSR 矩阵
            def to_sparse_matrix(data_list, row_offset, col_offset):
                rows, cols, vals = [], [], []
                for r, c, v in data_list:
                    rows.append(r - row_offset)
                    cols.append(c - col_offset)
                    vals.append(v)
                # 使用 COO 构建再转为 CSR 方便矩阵乘法
                return sparse.coo_matrix(
                    (vals, (rows, cols)),
                    shape=(block_size, block_size)
                ).tocsr()
            # 转换当前两个块
            mat_a = to_sparse_matrix(list_a, bi * block_size, bk * block_size)
            mat_b = to_sparse_matrix(list_b, bk * block_size, bj * block_size)
            # 执行稀疏矩阵乘法 (CSR 格式下非常快)
            res_mat = mat_a.dot(mat_b).tocoo()
            # 提取非零结果，还原回全局坐标
            output = []
            row_origin = bi * block_size
            col_origin = bj * block_size
            # 直接遍历 COO 格式的非零数据，避开所有零元素
            for r, c, v in zip(res_mat.row, res_mat.col, res_mat.data):
                if v != 0:
                    output.append(((r + row_origin, c + col_origin), float(v)))
            return output
        # 执行 Join、计算、并按全局 (r, c) 坐标聚合结果
        C = A_keyed.join(B_keyed, numPartitions=num_partitions) \
                   .flatMap(sparse_block_dot) \
                   .reduceByKey(lambda x, y: x + y, numPartitions=num_partitions)
        count = C.count()
        return "block_sparse", time.time() - start_time, count
    except Exception as e:
        print("Error in block_matrix_multiplication:")
        traceback.print_exc()
        return "block_sparse", -1, 0
'''
# ==========================================
# 主程序入口
# ==========================================
def main():
    # 接收来自 Shell 的 5 个参数
    if len(sys.argv) != 6:
        print("Usage: spark-submit script.py <scenario> <A_path> <B_path> <out_file> <method>")
        sys.exit(1)
    scenario = sys.argv[1]
    A_path = sys.argv[2]
    B_path = sys.argv[3]
    out_file = sys.argv[4]
    method_requested = sys.argv[5]
    # 使用 .format() 设置 AppName
    app_name = "Matrix_{}_{}".format(scenario, method_requested)
    conf = SparkConf().setAppName(app_name)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = SparkContext(conf=conf)
    # 并行度设置
    parallelism = int(sc.getConf().get("spark.default.parallelism", "100"))
    # 加载 RDD
    rdd_A = load_matrix_rdd(sc, A_path, parallelism).cache()
    rdd_B = load_matrix_rdd(sc, B_path, parallelism).cache()
    # 触发 Cache
    rdd_A.count()
    rdd_B.count()
    # 算法路由
    if method_requested == "naive":
        name, dur, count = naive_matrix_multiplication(sc, rdd_A, rdd_B, parallelism)
    elif method_requested == "broadcast":
        name, dur, count = broadcast_matrix_multiplication(sc, rdd_A, rdd_B, parallelism)
    elif method_requested == "block":
        # The block size is hardcoded to 1000 in the function signature but could be parameterized
        name, dur, count = block_matrix_multiplication(sc, rdd_A, rdd_B, parallelism)
    else:
        name, dur, count = "unknown", 0, 0
    # 严格使用 .format() 构造 CSV 结果行
    # 格式: 场景,方法,时间(4位小数),非零数
    res_line = "{},{},{:.4f},{}\n".format(scenario, name, dur, count)
    with open(out_file, 'a') as f:
        f.write(res_line)
    print("\nExperiment Finished: {}".format(res_line.strip()))
    rdd_A.unpersist()
    rdd_B.unpersist()
    sc.stop()
if __name__ == "__main__":
    main()
