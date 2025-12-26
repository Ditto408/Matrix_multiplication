#!/bin/bash

# ================= 基础配置 (严格保留) =================
BASE_DIR="/home/spark/work/Matrix_multiplication"
SPARK_SUBMIT="/home/spark/spark-2.3.2/bin/spark-submit"
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

    echo ">>> 运行场景: $SCENARIO | 算法: $METHOD"

    # 严格保持原始 spark-submit 参数
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
# 1. 场景：稠密矩阵 (500) - 全算法对比
# ==========================================================

#for m in "naive" "broadcast" "block"; do
#for m in  "block"; do
#    run_experiment "Dense_500" "$HDFS_ROOT/A_Dense_500" "$HDFS_ROOT/B_Dense_500" "$m" "$LOG_DIR/dense_500_${m}_$TS.log"
#    run_experiment "Dense_1000" "$HDFS_ROOT/A_Dense_1000" "$HDFS_ROOT/B_Dense_1000" "$m" "$LOG_DIR/dense_1000_${m}_$TS.log"
#    run_experiment "Dense_2000" "$HDFS_ROOT/A_Dense_2000" "$HDFS_ROOT/B_Dense_2000" "$m" "$LOG_DIR/dense_2000_${m}_$TS.log"
#done

# ==========================================================
# 2. 场景：固定稀疏度 0.01 - 全算法对比 (观察规模变化)
# ==========================================================
# 对 1000, 5000, 10000 三种规模分别进行三种算法测试
Sparsity="0.01"
for size in "1000"; do
#for size in "5000" "10000" "20000"; do
    #for m in "naive" "broadcast" "block"; do
    #for m in "block"; do
    for m in "broadcast"; do
        # 注意：如果 20000 规模下朴素算法太慢，可以在这里手动跳过，或者直接运行观察性能瓶颈
        run_experiment "Scale_${Sparsity}_$size" "$HDFS_ROOT/A_Scale_${Sparsity}_$size" "$HDFS_ROOT/B_Scale_${Sparsity}_$size" "$m" "$LOG_DIR/scale_${Sparsity}_${size}_${m}_$TS.log"
    done
done

# ==========================================================
# 3. 场景：固定规模 1w，不同稀疏度 - 全算法对比
# ==========================================================
#Scale="2000"
Scale="5000"
#for sp in "0.001" "0.05" "0.1"; do
#for sp in  "0.05"; do
    #for m in "naive" "broadcast" "block"; do
#    for m in "block"; do
#        run_experiment "Sparsity_${sp}_$Scale" "$HDFS_ROOT/A_Sparsity_${sp}_$Scale" "$HDFS_ROOT/B_Sparsity_${sp}_$Scale" "$m" "$LOG_DIR/sparsity_${sp}_${Scale}_${m}_$TS.log"
#    done
#done

# ==========================================================
# 4. 场景：广播对比场景 - 全算法对比 (验证广播优势)
# ==========================================================
# 测试特殊的长宽矩阵对
#PAIRS=("BroadcastPair1" "BroadcastPair2")
#for p in "${PAIRS[@]}"; do
#    # 这里的路径变量根据您的 HDFS 定义动态映射
#    if [ "$p" == "BroadcastPair1" ]; then
#        A_DIR="$HDFS_ROOT/A_BroadcastPair1_10000x1000_by_1000x512"
#        B_DIR="$HDFS_ROOT/B_BroadcastPair1_10000x1000_by_1000x512"
#    else
#        A_DIR="$HDFS_ROOT/A_BroadcastPair2_20000x64_by_64x20000"
#        B_DIR="$HDFS_ROOT/B_BroadcastPair2_20000x64_by_64x20000"
#    fi
#
#    for m in "naive" "broadcast" "block"; do
#        run_experiment "$p" "$A_DIR" "$B_DIR" "$m" "$LOG_DIR/${p}_${m}_$TS.log"
#    done
#done

echo "========================================="
echo "所有 14 个场景下的 3 种算法测试已全部提交"
echo "结果已记录在: $RESULT_FILE"
echo "========================================="
