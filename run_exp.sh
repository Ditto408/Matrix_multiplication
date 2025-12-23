#!/bin/bash

# 确保有执行权限: chmod +x run_exp.sh

# 0. 环境设置
# 请修改为你的 Spark 安装路径
SPARK_HOME=/usr/local/spark
# 请修改为你的 SystemDS jar 包路径
SYSTEMDS_JAR=/path/to/systemds.jar

DATA_DIR="./data"
LOG_DIR="./logs"
OUTPUT_HDFS="/user/ubuntu/output" # 修改为你的 HDFS 路径

mkdir -p $DATA_DIR
mkdir -p $LOG_DIR

# 1. 生成数据 (规模: 1000x1000, 稀疏度 0.1)
echo "Step 1: Generating Data..."
python src/generator.py 1000 1000 0.1 $DATA_DIR/matA.txt
python src/generator.py 1000 1000 0.1 $DATA_DIR/matB.txt

# 将数据上传到 HDFS (如果是伪分布式或集群模式)
# hadoop fs -put $DATA_DIR/matA.txt /input/
# hadoop fs -put $DATA_DIR/matB.txt /input/
# 为了演示，这里假设使用本地文件 file:// (注意：集群模式下所有节点都需要有该文件)
INPUT_A="file://$(pwd)/data/matA.txt"
INPUT_B="file://$(pwd)/data/matB.txt"

# 2. 运行 Naive Spark
echo "Step 2: Running Naive Spark..."
$SPARK_HOME/bin/spark-submit \
  --master spark://master-ip:7077 \
  --executor-memory 2G \
  --driver-memory 1G \
  src/naive_mult.py $INPUT_A $INPUT_B $OUTPUT_HDFS/naive \
  > $LOG_DIR/naive.log 2>&1

# 3. 运行 Optimized Spark
echo "Step 3: Running Optimized Spark..."
$SPARK_HOME/bin/spark-submit \
  --master spark://master-ip:7077 \
  --executor-memory 2G \
  --driver-memory 1G \
  src/opt_mult.py $INPUT_A $INPUT_B $OUTPUT_HDFS/opt \
  > $LOG_DIR/opt.log 2>&1

# 4. 运行 SystemDS (如果配置好了)
# echo "Step 4: Running SystemDS..."
# $SPARK_HOME/bin/spark-submit $SYSTEMDS_JAR -f src/systemds_script.dml \
#   -args $INPUT_A $INPUT_B $OUTPUT_HDFS/systemds \
#   > $LOG_DIR/systemds.log 2>&1

echo "Done! Check logs in $LOG_DIR"
# 简单打印结果时间
grep "Execution Time" $LOG_DIR/*.log