#!/bin/bash

# =================================================================
# 1. 基础环境配置 & 自动编译
# =================================================================
PROJECT_DIR="/home/spark/work/Matrix_Scala_232"
JAR_PATH="$PROJECT_DIR/target/scala-2.11/matrixmultiplication_2.11-0.1.jar"
MAIN_CLASS="MatrixMultiplicationOptimized"
HDFS_ROOT="hdfs://spark-master:9000/input"
RESULT_FILE="./scala_final_results.csv"

echo "==========================================================="
echo "🛠️  步骤1: 重新编译代码 (sbt clean package)..."
echo "==========================================================="
cd $PROJECT_DIR
sbt clean package
if [ $? -ne 0 ]; then
    echo "❌ 编译失败，脚本终止！"
    exit 1
fi
echo "✅ 编译成功！"

echo "==========================================================="
echo "🚀 步骤2: 开始批量执行实验 (Best Opt V6)"
echo "📅 时间: $(date)"
echo "📂 结果输出: $RESULT_FILE"
echo "==========================================================="

# =================================================================
# 2. 定义执行函数
# =================================================================
run_experiment() {
    local NAME=$1
    
    # 路径定义
    local PATH_A="${HDFS_ROOT}/A_${NAME}"
    local PATH_B="${HDFS_ROOT}/B_${NAME}"

    echo ""
    echo "-----------------------------------------------------------"
    echo "▶ 正在运行任务: [${NAME}]"
    echo "-----------------------------------------------------------"

    # ========================== 参数解析 ==========================
    # 核心策略：并行度设为 400
    # 原因：对于 20000x20000 且 blockSize=1000，正好有 400 个块。
    #      400 个并行度能让每个 Task 刚好处理一个块，没有调度浪费。
    #      对于 50000x50000，虽然有 2500 个块，但 400 并行度依然能稳健运行。
    # ==============================================================
    
    spark-submit \
      --class $MAIN_CLASS \
      --master spark://spark-master:7077 \
      --packages org.scalanlp:breeze_2.11:0.13.2,org.scalanlp:breeze-natives_2.11:0.13.2 \
      --driver-memory 2G \
      --executor-memory 4500M \
      --executor-cores 3 \
      --num-executors 2 \
      --conf "spark.executor.memoryOverhead=2048" \
      --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
      --conf "spark.kryoserializer.buffer.max=512m" \
      --conf "spark.memory.fraction=0.6" \
      --conf "spark.memory.storageFraction=0.3" \
      --conf "spark.network.timeout=1200s" \
      --conf "spark.shuffle.file.buffer=64k" \
      --conf "spark.shuffle.io.maxRetries=10" \
      --conf "spark.rpc.askTimeout=600s" \
      \
      --conf "spark.default.parallelism=400" \
      --conf "spark.sql.shuffle.partitions=400" \
      \
      $JAR_PATH \
      "$NAME" \
      "$PATH_A" \
      "$PATH_B" \
      "$RESULT_FILE" \
      "block_opt_best"
      
    if [ $? -eq 0 ]; then
        echo "✅ 任务 [${NAME}] 完成"
    else
        echo "❌ 任务 [${NAME}] 失败"
    fi
    
    sleep 5
}

# =================================================================
# 3. 执行顺序 (按从小到大，先易后难)
# =================================================================

# --- 1. 预热 (Warm-up) ---
echo ">>> 🔥 Warming up..."
run_experiment "Dense_500"

# --- 2. Group A: 稠密矩阵 ---
echo ">>> [Group A] Dense Matrices..."
run_experiment "Dense_1000"
run_experiment "Dense_2000"

# --- 3. Group C: 稀疏度测试 (Scale=5000) ---
echo ">>> [Group C] Sparsity Sensitivity..."
run_experiment "Sparsity_0.001_5000"
run_experiment "Sparsity_0.05_5000"
run_experiment "Sparsity_0.1_5000"

# --- 4. Group B: 规模增长 (Scale-up) ---
echo ">>> [Group B] Scale-up (Sparse 0.01)..."
# 小规模
run_experiment "Scale_0.01_500"
run_experiment "Scale_0.01_1000"
run_experiment "Scale_0.01_2000"
run_experiment "Scale_0.01_5000"
# 中大规模
run_experiment "Scale_0.01_10000"
run_experiment "Scale_0.01_20000"

# --- 5. 最终 BOSS: 压力测试 (5w) ---
echo ">>> [Stress Test] 50k Scale (Patience required)..."
run_experiment "Scale_0.01_50000"

echo "==========================================================="
echo "🎉 恭喜！全量实验结束。请查看 $RESULT_FILE"
echo "==========================================================="