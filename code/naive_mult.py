# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
import sys
import time

def main(input_a, input_b, output_path):
    conf = SparkConf().setAppName("NaiveMatrixMultiplication")
    sc = SparkContext(conf=conf)

    start_time = time.time()

    # 1. 读取并解析: A(i, k), B(k, j)
    # A 转换为 (k, (i, value))
    mat_a = sc.textFile(input_a).map(lambda line: line.split(',')) \
              .map(lambda x: (int(x[1]), (int(x[0]), float(x[2]))))
    
    # B 转换为 (k, (j, value))
    mat_b = sc.textFile(input_b).map(lambda line: line.split(',')) \
              .map(lambda x: (int(x[0]), (int(x[1]), float(x[2]))))

    # 2. Join: 结果为 (k, ((i, valA), (j, valB)))
    # 这是性能瓶颈，会产生巨大的 Shuffle
    joined = mat_a.join(mat_b)

    # 3. 计算乘积: ((i, j), valA * valB)
    mapped = joined.map(lambda x: ((x[1][0][0], x[1][1][0]), x[1][0][1] * x[1][1][1]))

    # 4. 聚合: reduceByKey
    result = mapped.reduceByKey(lambda x, y: x + y)

    # 触发 Action 并计时
    count = result.count() 
    # result.saveAsTextFile(output_path) # 如果文件太大，测试时注释掉这行，仅用 count 触发
    
    end_time = time.time()
    print("### Naive Execution Time: {:.4f} seconds".format(end_time - start_time))
    
    sc.stop()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])