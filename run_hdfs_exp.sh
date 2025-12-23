#!/bin/bash

# =================配置区域=================
SPARK_SUBMIT_CMD="/home/spark/spark-1.6.3/bin/spark-submit"
HDFS_INPUT="/input"
HDFS_OUTPUT="/output"
# 这里的 Master 地址请保持 yarn-client
MASTER="yarn-client"
MEM="1G"
CODE_DIR="code"
# =========================================

echo "========================================="
echo "开始运行基于 Hadoop HDFS 的实验 (Safe Mode)"
echo "========================================="

# 检查命令是否存在
if [ ! -f "$SPARK_SUBMIT_CMD" ]; then
    echo "❌ 错误: 找不到 $SPARK_SUBMIT_CMD"
    exit 1
fi

# 1. 运行 Naive 算法 (使用单行命令避免换行符错误)
echo "[1/2] Running Naive Implementation..."
hadoop fs -rm -r $HDFS_OUTPUT/naive > /dev/null 2>&1

$SPARK_SUBMIT_CMD --master $MASTER --executor-memory $MEM --driver-memory $MEM $CODE_DIR/naive_mult.py hdfs://spark-master:9000$HDFS_INPUT/matA.txt hdfs://spark-master:9000$HDFS_INPUT/matB.txt hdfs://spark-master:9000$HDFS_OUTPUT/naive

# 2. 运行 Optimized 算法
echo "[2/2] Running Optimized Implementation..."
hadoop fs -rm -r $HDFS_OUTPUT/opt > /dev/null 2>&1

$SPARK_SUBMIT_CMD --master $MASTER --executor-memory $MEM --driver-memory $MEM $CODE_DIR/opt_mult.py hdfs://spark-master:9000$HDFS_INPUT/matA.txt hdfs://spark-master:9000$HDFS_INPUT/matB.txt hdfs://spark-master:9000$HDFS_OUTPUT/opt

echo "========================================="
echo "✅ 实验结束！请查看上方输出的 Execution Time"
echo "========================================="