#!/bin/bash

# ================= 基础配置 =================
BASE_DIR="/home/spark/work/Matrix_multiplication"
SPARK_SUBMIT="/home/spark/spark-2.3.2/bin/spark-submit"

# 确保引用的是 code 目录下的 python 脚本
PYTHON_CODE="$BASE_DIR/code/matrix_ed3.py"

HDFS_ROOT="hdfs://spark-master:9000/input"
RESULTS_DIR="$BASE_DIR/results/ed3"
LOG_DIR="$BASE_DIR/logs/ed3"

mkdir -p "$RESULTS_DIR"
mkdir -p "$LOG_DIR"

TS=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULTS_DIR/experiment_results_$TS.txt"

# ==========================================================
# 核心函数：运行一个实验
# ==========================================================
run_experiment() {
    local SCENARIO=$1
    local A_PATH=$2
    local B_PATH=$3
    local METHOD=$4
    local LOG_FILE=$5

    # 检查 HDFS 输入文件是否存在
    if ! hadoop fs -test -e "$A_PATH"; then
        echo "⚠️ 跳过: 数据不存在 $A_PATH"
        return
    fi

    echo ">>> 运行场景: $SCENARIO | 算法: $METHOD"

    $SPARK_SUBMIT \
        --master spark://spark-master:7077 \
        --executor-memory 4G \
        --driver-memory 2G \
        --num-executors 3 \
        --executor-cores 3 \
        --conf spark.default.parallelism=24 \
        --conf spark.python.worker.reuse=true \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.kryoserializer.buffer.max=512m \
        --conf spark.memory.fraction=0.6 \
        $PYTHON_CODE \
        "$SCENARIO" \
        "$A_PATH/part-00000" \
        "$B_PATH/part-00000" \
        "$RESULT_FILE" \
        "$METHOD" \
        2>&1 | tee "$LOG_FILE"
}

# 初始化结果文件
echo "# scenario,method,time_sec,nnz" > "$RESULT_FILE"

# ==========================================================
# Group A: 稠密矩阵 (Dense Matrix)
# 对应生成脚本: run_gen 500/1000/2000 ... 1.0 "Dense_X"
# ==========================================================
echo "--- Starting Group A: Dense ---"
# 可选算法: "naive" "broadcast" "block"
for m in "block"; do
    for size in "500" "1000" "2000"; do
        SCENARIO="Dense_${size}"
        run_experiment "$SCENARIO" "$HDFS_ROOT/A_$SCENARIO" "$HDFS_ROOT/B_$SCENARIO" "$m" "$LOG_DIR/${SCENARIO}_${m}_$TS.log"
    done
done

# ==========================================================
# Group B: 规模增长 (Scale-up) - 固定稀疏度 0.01
# 对应生成脚本: run_gen 5000/10000/20000 ... 0.01 "Scale_0.01_X"
# ==========================================================
echo "--- Starting Group B: Scale-up (sp=0.01) ---"
Sparsity="0.01"

# 可选算法: "block" (推荐大矩阵使用), "broadcast" (中等矩阵)
for m in "block"; do
    for size in "5000" "10000" "20000"; do
        SCENARIO="Scale_${Sparsity}_${size}"
        run_experiment "$SCENARIO" "$HDFS_ROOT/A_$SCENARIO" "$HDFS_ROOT/B_$SCENARIO" "$m" "$LOG_DIR/${SCENARIO}_${m}_$TS.log"
    done
done

# ==========================================================
# Group C: 稀疏度敏感性 (Sparsity) - 固定规模 5000
# 对应生成脚本: run_gen 5000 ... 0.001/0.05/0.1 "Sparsity_X_5000"
# ==========================================================
echo "--- Starting Group C: Sparsity Sensitivity (Size=5000) ---"
Scale="5000"

for m in "block"; do
    for sp in "0.001" "0.05" "0.1"; do
        SCENARIO="Sparsity_${sp}_${Scale}"
        run_experiment "$SCENARIO" "$HDFS_ROOT/A_$SCENARIO" "$HDFS_ROOT/B_$SCENARIO" "$m" "$LOG_DIR/${SCENARIO}_${m}_$TS.log"
    done
done

# ==========================================================
# Group D: 压力测试 (Stress Test) - 50k x 50k
# 对应生成脚本: run_gen 50000 ... 0.01 "Scale_0.01_50000"
# ==========================================================
# echo "--- Starting Group D: Stress Test (50k) ---"
# 注意：50k 规模非常大，仅建议使用 block 算法，且可能需要较长时间
# SCENARIO="Scale_0.01_50000"
# run_experiment "$SCENARIO" "$HDFS_ROOT/A_$SCENARIO" "$HDFS_ROOT/B_$SCENARIO" "block" "$LOG_DIR/${SCENARIO}_block_$TS.log"

echo "========================================="
echo "所有生成脚本对应的场景测试已完成"
echo "结果已记录在: $RESULT_FILE"
echo "========================================="