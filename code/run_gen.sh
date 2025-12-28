#!/bin/bash

# ================= 配置区域 =================
# 1. 使用纯 Python 生成器
BASE_DIR="/home/spark/work/Matrix_multiplication"
GEN_CODE="$BASE_DIR/code/datagen.py"

# 2. 路径配置
HDFS_ROOT="hdfs://spark-master:9000/input"
LOCAL_DATA_ROOT="$BASE_DIR/data"

# ===========================================

# 创建本地数据目录
mkdir -p $LOCAL_DATA_ROOT

echo "========================================="
echo "🚀 开始生成实验数据 (全场景覆盖版)"
echo "策略: 本地生成 -> 上传 HDFS"
echo "========================================="

run_gen() {
    local ROWS=$1
    local COLS=$2
    local SPARSITY=$3
    local NAME=$4 

    # 定义文件名
    local FILE_A="${LOCAL_DATA_ROOT}/A_${NAME}.txt"
    local FILE_B="${LOCAL_DATA_ROOT}/B_${NAME}.txt"
    
    # 定义 HDFS 目标文件夹
    local HDFS_A="${HDFS_ROOT}/A_${NAME}"
    local HDFS_B="${HDFS_ROOT}/B_${NAME}"

    echo "-----------------------------------------"
    echo "正在处理组: [${NAME}]"
    echo "规模: ${ROWS}x${COLS} | 稀疏度: ${SPARSITY}"
    echo "-----------------------------------------"

    # 1. 本地生成 (串行执行，绝对稳)
    echo " [1/4] Generating Matrix A locally..."
    if [ ! -f "$FILE_A" ]; then
        python3 $GEN_CODE $ROWS $COLS $SPARSITY $FILE_A
    else
        echo "    -> 文件已存在，跳过生成。"
    fi

    echo " [2/4] Generating Matrix B locally..."
    if [ ! -f "$FILE_B" ]; then
        python3 $GEN_CODE $ROWS $COLS $SPARSITY $FILE_B
    else
        echo "    -> 文件已存在，跳过生成。"
    fi

    # 2. 清理 HDFS 旧数据
    echo " [3/4] Cleaning HDFS..."
    hadoop fs -rm -r $HDFS_A > /dev/null 2>&1
    hadoop fs -rm -r $HDFS_B > /dev/null 2>&1
    
    # 3. 上传到 HDFS
    echo " [4/4] Uploading to HDFS..."
    
    # 上传为文件夹结构 (part-00000) 以适配 Spark 读取逻辑
    hadoop fs -mkdir -p $HDFS_A
    hadoop fs -put $FILE_A $HDFS_A/part-00000
    
    hadoop fs -mkdir -p $HDFS_B
    hadoop fs -put $FILE_B $HDFS_B/part-00000

    echo "✅ 组 [${NAME}] 全部完成！"
}

# ===========================================
# 场景 A：稠密矩阵 (Dense Matrix)
# ===========================================
# 目的：测试 SystemML 和 Spark 在无稀疏优化下的“硬计算”能力
echo ">>> [Group A] 执行稠密矩阵生成..."
run_gen 500 500 1.0 "Dense_500"
run_gen 1000 1000 1.0 "Dense_1000"
run_gen 2000 2000 1.0 "Dense_2000"

# ===========================================
# 场景 B：规模增长 (Scale-up) - 固定稀疏度 0.01
# ===========================================
# 目的：测试数据量增大时的系统扩展性
echo ">>> [Group B] 执行规模增长生成 (sp=0.01)..."
run_gen 5000 5000 0.01 "Scale_0.01_5000"
run_gen 10000 10000 0.01 "Scale_0.01_10000"
run_gen 20000 20000 0.01 "Scale_0.01_20000"

# 🚀【新增】压力测试 (5w x 5w)
# 估算：50000*50000*0.01 = 2500万非零元素。文本文件约 500MB~600MB。
# 这是逼出 SystemML Shuffle 的关键。
echo ">>> [Stress Test] 生成 5w 规模数据 (可能需要几分钟)..."
run_gen 50000 50000 0.01 "Scale_0.01_50000"

# ===========================================
# 场景 C：稀疏度敏感性 (Sparsity) - 固定规模 5000
# ===========================================
# 目的：在 5000 规模下，观察不同稀疏度对广播/计算的影响
echo ">>> [Group C] 执行稀疏度变化生成 (Scale=5000)..."
run_gen 5000 5000 0.001 "Sparsity_0.001_5000"
run_gen 5000 5000 0.05  "Sparsity_0.05_5000"
run_gen 5000 5000 0.1   "Sparsity_0.1_5000"

echo "========================================="
echo "🎉 所有数据生成任务结束！"
echo "🎉 HDFS 数据已就绪，请更新 run_experiments.sh 对应路径。"
echo "========================================="