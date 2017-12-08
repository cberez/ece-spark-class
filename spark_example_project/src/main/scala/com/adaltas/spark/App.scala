package com.adaltas.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setAppName("spark-example")
    val sc = new SparkContext

    println(s"APP-ID=${sc.applicationId}")

    sc.stop()
  }
}
