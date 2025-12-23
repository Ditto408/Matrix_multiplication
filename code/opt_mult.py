# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
import sys
import time

def main(input_a, input_b, output_path):
    conf = SparkConf().setAppName("OptimizedMatrixMultiplication")
    # 开启 Kryo 序列化，Spark 1.6 中能显著减少内存占用
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = SparkContext(conf=conf)

    start_time = time.time()
    
    # 优化点1: 设置并行度 (根据你的 3 台机器，假设每台 4 核，共 12 核，设置为 36)
    num_partitions = 36 

    # 读取 A: (k, (i, val))
    # 优化点2: 过滤掉 0 值 (稀疏矩阵核心优化)
    mat_a = sc.textFile(input_a).map(lambda line: line.split(',')) \
              .filter(lambda x: float(x[2]) != 0) \
              .map(lambda x: (int(x[1]), (int(x[0]), float(x[2])))) \
              .partitionBy(num_partitions) # 优化点3: 预分区
    
    # 读取 B: (k, (j, val))
    mat_b = sc.textFile(input_b).map(lambda line: line.split(',')) \
              .filter(lambda x: float(x[2]) != 0) \
              .map(lambda x: (int(x[0]), (int(x[1]), float(x[2])))) \
              .partitionBy(num_partitions)

    # Join
    joined = mat_a.join(mat_b)

    # 计算
    mapped = joined.map(lambda x: ((x[1][0][0], x[1][1][0]), x[1][0][1] * x[1][1][1]))

    # 优化点4: reduceByKey 会在 Map 端进行预聚合 (Combiner)，大大减少 Shuffle 写出量
    result = mapped.reduceByKey(lambda x, y: x + y)

    # 触发计算
    count = result.count()
    # result.saveAsTextFile(output_path) 
    
    end_time = time.time()
    print("### Optimized Execution Time: {:.4f} seconds".format(end_time - start_time))
    
    sc.stop()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])