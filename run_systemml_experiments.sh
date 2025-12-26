#!/bin/bash

# ================= 基础配置 =================
BASE_DIR="/home/spark/work/zh_work"
SPARK_SUBMIT="/home/spark/spark-2.3.2/bin/spark-submit"

# SystemML Jar 包路径
SYSTEMML_JAR="/home/spark/systemml-1.2.0-bin/lib/systemml-1.2.0.jar"
DML_SCRIPT="$BASE_DIR/code/matmul.dml"

HDFS_ROOT="hdfs://spark-master:9000/input"
RESULTS_DIR="$BASE_DIR/results"
LOG_DIR="$BASE_DIR/SDS_logs"

mkdir -p "$RESULTS_DIR"
mkdir -p "$LOG_DIR"

TS=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULTS_DIR/systemml_results_$TS.txt"

# ==========================================================
# 核心函数：运行 SystemML 实验 (支持动态维度)
# ==========================================================
run_systemml() {
    local SCENARIO=$1
    local A_PATH=$2
    local B_PATH=$3
    local LOG_FILE=$4
    # 【关键修改】接收动态维度参数
    local ROWS=$5
    local COLS=$6

    echo ">>> [SystemML] 运行场景: $SCENARIO (${ROWS}x${COLS})"

    START_TIME=$(date +%s%N) 

    $SPARK_SUBMIT \
        --name "SDS_${SCENARIO}" \
        --master spark://spark-master:7077 \
        --executor-memory 5G \
        --driver-memory 2G \
        --num-executors 4 \
        --executor-cores 4 \
        --conf spark.default.parallelism=128 \
        --conf spark.driver.maxResultSize=2g \
        $SYSTEMML_JAR \
        -f $DML_SCRIPT \
        -exec spark \
        -stats \
        -nvargs Ain="$A_PATH" Bin="$B_PATH" rows=$ROWS cols=$COLS \
        > "$LOG_FILE" 2>&1

    END_TIME=$(date +%s%N)
    
    DURATION=$(echo "scale=4; ($END_TIME - $START_TIME) / 1000000000" | bc)

    # 尝试从日志抓取精确执行时间
    SDS_TIME=$(grep "Total execution time" "$LOG_FILE" | awk '{print $4}')
    
    if [ -z "$SDS_TIME" ]; then
        SDS_TIME=$DURATION
        echo "   (注意: 未在日志中找到 SystemML 内部计时，使用 Shell 计时)"
    fi

    # 写入结果：场景, 方法, 时间
    echo "${SCENARIO},SystemML,${SDS_TIME}" >> "$RESULT_FILE"
    
    echo "✅ 完成: ${SCENARIO} | 耗时: ${SDS_TIME}s"
    echo "   日志: $LOG_FILE"
    echo "------------------------------------------------------------"
}

# 初始化结果文件
echo "scenario,method,time_sec" > "$RESULT_FILE"

# ==========================================================
# 场景 A: 稠密矩阵 (Dense)
# ==========================================================
echo ">>> Running Group A: Dense Matrices..."
for size in "500" "1000" "2000"; do
    SCENARIO="Dense_${size}"
    A_DIR="$HDFS_ROOT/A_${SCENARIO}"
    B_DIR="$HDFS_ROOT/B_${SCENARIO}"
    
    if hadoop fs -test -e "$A_DIR"; then
        run_systemml "$SCENARIO" "$A_DIR" "$B_DIR" "$LOG_DIR/sds_${SCENARIO}_$TS.log" $size $size
    fi
done

# ==========================================================
# 场景 B: 规模增长 (Scale-up, Sparsity=0.01)
# ==========================================================
echo ">>> Running Group B: Scale-up (sp=0.01)..."
Sparsity="0.01"
for size in "5000" "10000" "20000"; do
    SCENARIO="Scale_${Sparsity}_${size}"
    A_DIR="$HDFS_ROOT/A_${SCENARIO}"
    B_DIR="$HDFS_ROOT/B_${SCENARIO}"
    
    if hadoop fs -test -e "$A_DIR"; then
        run_systemml "$SCENARIO" "$A_DIR" "$B_DIR" "$LOG_DIR/sds_${SCENARIO}_$TS.log" $size $size
    fi
done

# ==========================================================
# 场景 C: 稀疏度敏感性 (Scale=5000)
# ==========================================================
echo ">>> Running Group C: Sparsity Sensitivity (size=5000)..."
Scale="5000"
for sp in "0.001" "0.05" "0.1"; do
    SCENARIO="Sparsity_${sp}_${Scale}"
    A_DIR="$HDFS_ROOT/A_${SCENARIO}"
    B_DIR="$HDFS_ROOT/B_${SCENARIO}"
    
    if hadoop fs -test -e "$A_DIR"; then
        run_systemml "$SCENARIO" "$A_DIR" "$B_DIR" "$LOG_DIR/sds_${SCENARIO}_$TS.log" $Scale $Scale
    fi
done

# ==========================================================
# 场景 D: 压力测试 (50k) - 选跑
# ==========================================================
echo ">>> Running Group D: Stress Test (50k)..."
SCENARIO="Scale_0.01_50000"
A_DIR="$HDFS_ROOT/A_${SCENARIO}"
B_DIR="$HDFS_ROOT/B_${SCENARIO}"
SIZE="50000"

if hadoop fs -test -e "$A_DIR"; then
    run_systemml "$SCENARIO" "$A_DIR" "$B_DIR" "$LOG_DIR/sds_${SCENARIO}_$TS.log" $SIZE $SIZE
else
    echo "⚠️ 跳过: 50k 数据未找到 (可能尚未生成)"
fi

echo "========================================="
echo "🎉 SystemML 实验结束"
echo "📊 结果已保存: $RESULT_FILE"
echo "========================================="