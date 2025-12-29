import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}
import org.apache.spark.rdd.RDD
import breeze.linalg.{CSCMatrix => SparseMatrix, _} 
import java.io.FileWriter
import scala.util.Try
import scala.collection.mutable.ArrayBuffer 

case class MatrixEntry(row: Long, col: Long, value: Double) extends Serializable

object MatrixMultiplicationOptimized {

  def parseMatrixLine(line: String): Option[MatrixEntry] = {
    Try {
      val parts = line.trim.split(Array(' ', ',', '\t')).filter(_.nonEmpty)
      if (parts.length == 3) {
        Some(MatrixEntry(parts(0).toLong, parts(1).toLong, parts(2).toDouble))
      } else None
    }.getOrElse(None)
  }

  // 🌟 最佳配置：blockSize 默认为 1000
  def blockMultiplication(sc: SparkContext, A: RDD[MatrixEntry], B: RDD[MatrixEntry], parallelism: Int, blockSize: Int = 1000): (String, Double, Long) = {
    val startTime = System.nanoTime()
    
    // 使用 HashPartitioner 保证 Shuffle 的稳定性
    val partitioner = new HashPartitioner(parallelism)

    // 1. 构建 A 块 (使用 ArrayBuffer 消除 O(N) 拼接开销)
    val A_blocks = A.map { e =>
      (( (e.row / blockSize).toInt, (e.col / blockSize).toInt ), (e.row % blockSize, e.col % blockSize, e.value))
    }.aggregateByKey((new ArrayBuffer[Int], new ArrayBuffer[Int], new ArrayBuffer[Double]), partitioner)(
      (agg, e) => {
        agg._1 += e._1.toInt; agg._2 += e._2.toInt; agg._3 += e._3
        agg
      },
      (agg1, agg2) => {
        agg1._1 ++= agg2._1; agg1._2 ++= agg2._2; agg1._3 ++= agg2._3
        agg1
      }
    ).mapValues { case (rows, cols, vals) =>
      val builder = new SparseMatrix.Builder[Double](blockSize, blockSize)
      (rows.iterator zip cols.iterator zip vals.iterator).foreach { case ((r, c), v) => builder.add(r, c, v) }
      builder.result()
    }.map { case ((bi, bk), mat) => (bk, (bi, mat)) } 

    // 2. 构建 B 块
    val B_blocks = B.map { e =>
      (( (e.row / blockSize).toInt, (e.col / blockSize).toInt ), (e.row % blockSize, e.col % blockSize, e.value))
    }.aggregateByKey((new ArrayBuffer[Int], new ArrayBuffer[Int], new ArrayBuffer[Double]), partitioner)(
      (agg, e) => {
        agg._1 += e._1.toInt; agg._2 += e._2.toInt; agg._3 += e._3
        agg
      },
      (agg1, agg2) => {
        agg1._1 ++= agg2._1; agg1._2 ++= agg2._2; agg1._3 ++= agg2._3
        agg1
      }
    ).mapValues { case (rows, cols, vals) =>
      val builder = new SparseMatrix.Builder[Double](blockSize, blockSize)
      (rows.iterator zip cols.iterator zip vals.iterator).foreach { case ((r, c), v) => builder.add(r, c, v) }
      builder.result()
    }.map { case ((bk, bj), mat) => (bk, (bj, mat)) }

    // 3. 计算与聚合
    val count = A_blocks.join(B_blocks, partitioner) 
      .flatMap { case (_, ((bi, matA), (bj, matB))) =>
        val resMat = matA * matB
        // 🌟 核心优化：过滤掉全零矩阵，大幅减少 Shuffle Write
        if (resMat.activeSize == 0) Iterator.empty else Iterator(((bi, bj), resMat))
      }
      .reduceByKey(partitioner, (m1, m2) => m1 + m2) 
      .map { case (_, finalBlock) => finalBlock.activeSize.toLong }
      .reduce(_ + _)

    val duration = (System.nanoTime() - startTime) / 1e9
    ("block_best_v6", duration, count)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 5) sys.exit(1)
    val scenario = args(0); val aPath = args(1); val bPath = args(2); val outFile = args(3); val method = args(4)

    val conf = new SparkConf()
      .setAppName(s"Matrix_Opt_$scenario")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 显式注册 ArrayBuffer，进一步提升性能
      .registerKryoClasses(Array(classOf[MatrixEntry], classOf[SparseMatrix[Double]], classOf[ArrayBuffer[Any]]))

    val sc = new SparkContext(conf)
    val parallelism = sc.getConf.get("spark.default.parallelism", "400").toInt

    val rddA = sc.textFile(aPath, parallelism).flatMap(parseMatrixLine)
    val rddB = sc.textFile(bPath, parallelism).flatMap(parseMatrixLine)

    // 锁定 BlockSize 为 1000
    val blockSize = 1000

    val (resMethod, duration, count) = blockMultiplication(sc, rddA, rddB, parallelism, blockSize)

    val resLine = s"$scenario,$resMethod,${"%.4f".format(duration)},$count\n"
    val fw = new FileWriter(outFile, true)
    try { fw.write(resLine) } finally { fw.close() }

    println(s"Experiment Finished: ${resLine.trim}")
    sc.stop()
  }
}