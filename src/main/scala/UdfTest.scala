import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import scala.annotation.tailrec

object UdfTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config(new SparkConf()
        .setMaster("local[*]")
      )
      .appName("Spark UDF Perf Test")
      .getOrCreate()

    val getFibonacci: Int => Seq[Int] = (index: Int) => {
      Seq(fib(index, prev = 1, current = 0),
        fib(index + 1, prev = 1, current = 0),
        fib(index + 2, prev = 1, current = 0))
    }

    val udfFib = udf(getFibonacci)

    spark.range(1, 500000)
      .withColumn("fib", udfFib(col("id")))
      .withColumn("fib2", explode(col("fib")))
      .collect()

    Thread.sleep(10000000L)
  }

  @tailrec
  def fib(index: Int, prev: Int, current: Int): Int = {
    if (index == 0) {
      current
    } else {
      fib(index - 1, prev = prev + current, current = prev)
    }
  }
}