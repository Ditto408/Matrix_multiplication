#!/bin/bash

# ================= 配置区域 =================
# 1. 获取当前目录
BASE_DIR=$(cd "$(dirname "$0")"; pwd)

# 2. 【关键修复 1】指定 Hadoop 配置路径 (Spark 找 YARN 必需)
export HADOOP_CONF_DIR=/home/spark/hadoop-2.6.5/etc/hadoop

# 3. 【关键修复 2】强制指定 Python 路径 (解决 NumPy ImportError)
export PYSPARK_PYTHON=/usr/bin/python
export PYSPARK_DRIVER_PYTHON=/usr/bin/python

# 4. Spark 和 SystemML 路径
SPARK_SUBMIT="/home/spark/spark-1.6.3/bin/spark-submit"
SYSTEMML_JAR="/home/spark/systemml-1.2.0-bin/lib/systemml-1.2.0.jar"

# 5. 代码与数据路径
SPARK_CODE="$BASE_DIR/code/block_mult.py"
DML_CODE="$BASE_DIR/code/matmul.dml"
HDFS_A="hdfs://spark-master:9000/input/matA_large.txt"
HDFS_B="hdfs://spark-master:9000/input/matB_large.txt"
HDFS_OUT="/output"

# 6. 日志
LOG_DIR="$BASE_DIR/logs"
mkdir -p $LOG_DIR
TS=$(date +%Y%m%d_%H%M%S)
# ===========================================

echo "========================================="
echo "开始运行进阶对比实验"
echo "Hadoop配置: $HADOOP_CONF_DIR"
echo "Python环境: $PYSPARK_PYTHON"
echo "========================================="

# 检查文件
if [ ! -f "$DML_CODE" ]; then echo "❌ 找不到 DML 文件"; exit 1; fi
if [ ! -f "$SPARK_CODE" ]; then echo "❌ 找不到 Python 代码"; exit 1; fi

# --- 1. 运行 Spark Block Optimized ---
LOG_SPARK="$LOG_DIR/spark_block_$TS.log"
echo "[1/2] Running Spark Block-Matrix Implementation..." 
echo "日志: $LOG_SPARK"
hadoop fs -rm -r $HDFS_OUT/spark_block > /dev/null 2>&1

$SPARK_SUBMIT \
  --master yarn-client \
  --executor-memory 1G \
  --driver-memory 1G \
  --num-executors 4 \
  $SPARK_CODE $HDFS_A $HDFS_B $HDFS_OUT/spark_block \
  > $LOG_SPARK 2>&1

echo "✅ Spark 任务完成 (请检查日志)"

# --- 2. 运行 SystemDS (SystemML) ---
LOG_SDS="$LOG_DIR/systemds_$TS.log"
echo "[2/2] Running SystemDS Implementation..."
echo "日志: $LOG_SDS"

$SPARK_SUBMIT \
  --class org.apache.sysml.api.DMLScript \
  --master yarn-client \
  --executor-memory 1G \
  --driver-memory 1G \
  --num-executors 4 \
  $SYSTEMML_JAR \
  -f $DML_CODE \
  -nvargs Ain=$HDFS_A Bin=$HDFS_B \
  > $LOG_SDS 2>&1

echo "✅ SystemDS 任务完成"

echo "========================================="
echo "实验结束！请运行: python analyze_logs.py"
echo "========================================="