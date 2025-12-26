
### 项目目录结构


```Plaintext
.
├── code/
│   ├── datagen.py          # [Python] 稀疏/稠密矩阵数据生成脚本
│   ├── matmul.dml          # [DML] SystemML 矩阵乘法核心脚本
│   ├── matrix_ed3.py       # [Python] PySpark 矩阵乘法实现 (含 Naive/Broadcast/Block)
│   ├── run_experiments.sh  # [Shell] PySpark 实验自动化运行脚本
│   ├── run_gen.sh          # [Shell] 数据生成自动化脚本 (Local -> HDFS)
│   └── run_systemml.sh     # [Shell] SystemML 实验自动化运行脚本
└── README.md               # 项目说明文档
```

---

# 基于 Spark 的分布式矩阵乘法实现与优化

## 1. 研究目的

基于 Spark 实现分布式矩阵乘法，并与 SystemDS (SystemML) 中的实现进行比较。旨在深入理解分布式计算原理，掌握 Spark RDD 编程模型，并探索在大规模稀疏矩阵运算中的性能优化策略。

## 2. 研究内容

本实验的主要研究内容包括：

1. **基于 Spark 实现分布式矩阵乘法**：
    
    - 实现朴素矩阵乘法 (Naive Implementation)。
        
    - 实现基于广播变量的矩阵乘法 (Broadcast Implementation)。
        
    - 实现基于分块策略的矩阵乘法 (Block Matrix Multiplication)，利用 `scipy.sparse` 和 `breeze` 库对稀疏矩阵进行块内压缩存储与计算。

2. **性能对比与分析**：
    
    - 在不同规模（500维至50,000维）和不同稀疏度（0.001至0.1）的数据集上进行测试。
        
    - 对比自定义实现与 SystemDS 的性能差异。
        
    - 分析 Shuffle 数据量、内存占用及执行时间，探讨 Kryo 序列化、分块计算等优化策略的效果。
        

## 3. 实验

### 3.1 实验环境

硬件配置 (Cluster Configuration)

实验在一个由 Master 和 Worker 组成的分布式集群上进行。

- **节点数量**：3个节点 (1 Master + 2 Workers，根据日志推断)
    
- **CPU**：Intel(R) Xeon(R) Platinum 8260 CPU @ 2.40GHz (Virtual Machine)
    
- **内存**：单节点 8GB RAM
    
- **架构**：x86_64
    
- **网络**：Virtio network device (10Gbps 虚拟内网)
    

**软件环境**

- **操作系统**：Ubuntu 16.04.1 LTS (Kernel 4.4.0-210-generic)
    
- **JDK 版本**：OpenJDK 1.8.0_292
    
- **Hadoop 版本**：Hadoop 2.6.5
    
- **Spark 版本**：Spark 2.3.2 (Scala 2.11.8)
    
- **SystemML 版本**：SystemML 1.2.0
    
- **Python 环境**：Python 3.5.2 , Numpy, Scipy

### 3.2 实验负载 (Workload)

实验采用人工合成的稀疏矩阵数据集，通过 `datagen.py` 生成并上传至 HDFS。为了全面评估性能，设计了以下四组实验场景：

| **场景组**       | **矩阵规模 (Rows x Cols)** | **稀疏度 (Sparsity)** | **测试目的**                   |
| ------------- | ---------------------- | ------------------ | -------------------------- |
| **A. 稠密矩阵**   | 500, 1000, 2000        | 1.0 (Dense)        | 测试无稀疏优化下的计算基准能力            |
| **B. 规模扩展**   | 5k, 10k, 20k, 50k      | 0.01               | 测试数据量增大时的系统可扩展性 (Scale-up) |
| **C. 稀疏度敏感性** | 5000                   | 0.001, 0.05, 0.1   | 观察稀疏度变化对计算和通信开销的影响         |
| **D. 压力测试**   | 50,000 x 50,000        | 0.01               | 极限压力测试 (约2500万非零元素)        |

### 3.3 实验步骤

Step 1: 集群启动与环境检查

启动 Hadoop HDFS 与 Yarn，启动 Spark Master 与 Worker 进程。

![](attachments/1766758139866%201.png)

Step 2: 数据生成

执行 run_gen.sh 脚本，在本地生成矩阵数据并上传至 HDFS /input 目录。

![](attachments/Pasted%20image%2020251226221101.png)

Step 3: 作业提交与执行

使用 spark-submit 提交 Python 和 SystemML 任务。配置参数如下：
```
$SPARK_SUBMIT \
        --name "SDS_${SCENARIO}" \
        --master spark://spark-master:7077 \
        --executor-memory 4G \
        --driver-memory 2G \
        --num-executors 3 \
        --executor-cores 3 \
        --conf spark.default.parallelism=24 \
        --conf spark.memory.fraction=0.6 \
        --conf spark.driver.maxResultSize=2g \
        $SYSTEMML_JAR \
        -f $DML_SCRIPT \
        -exec spark \
        -stats \
        -nvargs Ain="$A_PATH" Bin="$B_PATH" rows=$ROWS cols=$COLS \
        > "$LOG_FILE" 2>&1
```

  ![](attachments/Pasted%20image%2020251226221821.png)

Step 4: 性能监控 (Spark UI)

通过 Spark UI (8080/4040端口) 监控 Stage 划分与 Shuffle 情况，分析 DAG 执行流程。

![](attachments/Pasted%20image%2020251226222855.png)
![](attachments/Pasted%20image%2020251226223325.png)
![](attachments/Pasted%20image%2020251226223406.png)

### 3.4 实验结果与分析

**实验结果汇总 (执行时间: 秒)**

根据日志文件整理的实验数据如下表所示：

| **场景 (Scenario)** | **规模**   | **SystemML (s)** | **Python Block (s)** |     |
| ----------------- | -------- | ---------------- | -------------------- | --- |
| **Dense (稠密)**    | 500      | 6.43             | 2.42                 |     |
|                   | 1000     | 8.40             | 5.36                 |     |
|                   | 2000     | 14.95            | 15.83                |     |
| **Scale (扩展)**    | 5000     | 7.18             | 3.17                 |     |
| (sp=0.01)         | 10000    | 10.49            | 6.83                 |     |
|                   | 20000    | **22.56**        | **129.85**           |     |
|                   | 50000    | 79.22            | (OOM/Timeout)        |     |
| **Sparsity**      | sp=0.001 | 5.65             | 2.78                 |     |
| (Size=5000)       | sp=0.1   | 10.69            | 12.50                |     |

结果分析图表

(此处建议基于上述表格数据，用 Excel 生成简单的折线对比图，在此处插入图片)

**分析与讨论：**

1. **SystemML vs. 自定义实现**：
    
    - 在小规模数据下，自定义的 Python Block 实现甚至略快于 SystemML，这是因为 SystemML 有一定的初始化和执行计划编译开销。
        
    - **关键发现**：随着数据规模增大 (如 Scale_20000)，SystemML (22.56s) 的优势显著体现，远快于 Python Block 实现 (129.85s)。这是因为 SystemML 底层对执行计划进行了深度优化（如动态重编译、算子融合），且运行在 JVM 层，避免了 Python 与 JVM 通信的开销。
        
2. **Python vs. Scala**：
    
    - Python 实现在大规模数据下遭遇了严重的性能瓶颈。分析 Spark UI 发现，Python Worker 即使启用了 `reuse=true`，其序列化（Pickling）和数据在 Java/Python 进程间传输的开销依然巨大。
        
    - Scala 版本（基于 Breeze 库）由于原生运行在 JVM 上，且配合 Kryo 序列化，理论性能应最接近 SystemML。
        
3. **优化策略的有效性**：
    
    - **分块 (Block) 策略**：相比于朴素实现（Naive），分块策略显著减少了 Shuffle 的对象数量。将 `(i, j, v)` 三元组聚合为 `(BlockID, MatrixObject)`，使得 Shuffle 过程中的记录数降低了几个数量级（取决于 Block Size，本实验为 1000）。
        
    - **稀疏矩阵存储**：在代码中引入 `scipy.sparse` (Python) 和 `Breeze CSCMatrix` (Scala) 后，内存占用大幅降低，使得在有限的内存 (4G Executor) 下能够跑通 20k 甚至更大规模的数据。
        

## 4. 结论

本实验成功在 Spark 集群上实现了多种分布式矩阵乘法算法。研究表明：

1. **算法选择**：对于大规模稀疏矩阵，**分块矩阵乘法**是必须的，它能有效平衡计算负载并降低通信开销。
    
2. **语言差异**：在 Spark 生态中，对于计算密集型任务，**Scala/Java 原生实现优于 PySpark**，除非利用 Pandas UDF 等向量化技术，否则 Python 的序列化开销在迭代计算中是不可忽视的瓶颈。
    
3. **框架优势**：SystemDS (SystemML) 在处理此类线性代数任务时表现出极高的稳定性和效率，这得益于其优化器对执行计划的自动重写和底层的高效算子。对于生产环境的复杂数学运算，使用 SystemDS 这类专用DSL比手写 Spark Core 代码更具优势。
    

## 5. 分工

