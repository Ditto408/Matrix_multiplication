#!/bin/bash

# ================= 配置区域 =================
BASE_DIR=$(cd "$(dirname "$0")"; pwd)

# 【关键修改】指向 Spark 2.3
# 如果你的软链接叫 spark2，路径就是这个；如果不是，请手动修改
SPARK_SUBMIT="$HOME/spark2/bin/spark-submit"

# SystemML Jar (保持不变)
SYSTEMML_JAR="$HOME/systemml-1.2.0-bin/lib/systemml-1.2.0.jar"

# 强制使用 Python 3 (解决 NumPy 和版本兼容问题)
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# 代码与数据
SPARK_CODE="$BASE_DIR/code/block_mult.py"
DML_CODE="$BASE_DIR/code/matmul.dml"
HDFS_A="hdfs://spark-master:9000/input/matA_large.txt"
HDFS_B="hdfs://spark-master:9000/input/matB_large.txt"
HDFS_OUT="/output"

# 日志
LOG_DIR="$BASE_DIR/logs"
mkdir -p $LOG_DIR
TS=$(date +%Y%m%d_%H%M%S)
# ===========================================

echo "========================================="
echo "🚀 运行环境: Spark 2.3 + Python 3"
echo "Spark命令: $SPARK_SUBMIT"
echo "========================================="

# 双重检查 Spark 版本 (防止又跑成 1.6)
if [[ "$SPARK_SUBMIT" == *"spark-1.6"* ]]; then
    echo "❌ 错误: 脚本依然指向 Spark 1.6！请修改 SPARK_SUBMIT 变量。"
    exit 1
fi

# 检查文件
if [ ! -f "$SPARK_SUBMIT" ]; then
    echo "❌ 错误: 找不到 Spark 2.3 启动命令: $SPARK_SUBMIT"
    echo "请检查是否创建了 ~/spark2 软链接，或修改脚本中的路径。"
    exit 1
fi

# --- 运行 SystemDS (SystemML) ---

LOG_SDS="$LOG_DIR/systemds_spark_$TS.log"
echo "Running SystemDS Implementation (Forced Spark Mode)..."
echo "Logs: $LOG_SDS"

$SPARK_SUBMIT \
  --class org.apache.sysml.api.DMLScript \
  --master yarn-client \
  --executor-memory 1G \
  --driver-memory 1G \
  --num-executors 4 \
  $SYSTEMML_JAR \
  -f $DML_CODE \
  -exec spark \
  -nvargs Ain=$HDFS_A Bin=$HDFS_B \
  > $LOG_SDS 2>&1


echo "✅ SystemDS 任务结束"

echo "========================================="
echo "实验完成！请运行 python analyze_logs.py"