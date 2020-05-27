package com.spark_hotpot.dt.abc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
//import scala.collection.JavaConverters._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import java.io.IOException
import scala.util.Try

object MyFileUtil {
  @throws[IOException]
  def copyMergeWithHeader(srcFS: FileSystem, srcDir: Path, dstFS: FileSystem, dstFile0: Path, deleteSource: Boolean, conf: Configuration, header: String): Boolean = {
    val dstFile = checkDest(srcDir.getName, dstFS, dstFile0, false)
    if (!srcFS.getFileStatus(srcDir).isDir) false
    else {
      val out = dstFS.create(dstFile)
      if (header != null) out.write((header + "\n").getBytes("UTF-8"))
      try {
        val contents = srcFS.listStatus(srcDir)
        var i = 0
        while ( {
          i < contents.length
        }) {
          if (!contents(i).isDir) {
            val in = srcFS.open(contents(i).getPath)
            try
              IOUtils.copyBytes(in, out, conf, false)
            finally in.close()
          }

          {
            i += 1; i
          }
        }
      } finally out.close()
      if (deleteSource) srcFS.delete(srcDir, true)
      else true
    }
  }

  @throws[IOException]
  private def checkDest(srcName: String, dstFS: FileSystem, dst: Path, overwrite: Boolean): Path = {
    if (dstFS.exists(dst)) {
      val sdst = dstFS.getFileStatus(dst)
      if (sdst.isDir) {
        if (null == srcName) throw new IOException("Target " + dst + " is a directory")
        return checkDest(null.asInstanceOf[String], dstFS, new Path(dst, srcName), overwrite)
      }
      if (!overwrite) throw new IOException("Target " + dst + " already exists")
    }
    dst
  }
}

class mainClass(val spark: SparkSession, val partitionNum:Int) {
  def DateAdd(day:String, dDay:Int):String = {
    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(fmt.parse(day))
    cal.add(Calendar.DATE, dDay)
    fmt.format(cal.getTime)
  }

  def isWeekday(day:String):Boolean = {
    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    val fmtDow = new SimpleDateFormat("u")
    val cal = Calendar.getInstance()
    cal.setTime(fmt.parse(day))
    val dayDow = fmtDow.format(cal.getTime).toInt
    val isWeekday = dayDow<6
    isWeekday
  }

  def readCsvFromFTP(SFTP_HOST:String,SFTP_USER:String,SFTP_PWD:String,readFileFromFolder:String,fileName:String,header:String): DataFrame = {
    spark.read.
      format("com.springml.spark.sftp").
      option("host", SFTP_HOST).
      option("username", SFTP_USER).
      option("password", SFTP_PWD).
      option("fileType", "csv").
      option("delimiter", ",").
      option("header",header).
      option("inferSchema", "true").
      load(readFileFromFolder+fileName)
  }

  def isFileOutputCsv (day:String,readFileFromFolder:String,filenameRoot:String): Boolean = {
    import spark.implicits._

    println("Checking output on ftp...")
    val SFTP_HOST= "xxxxxx.com"
    val SFTP_USER = "xxxxx"
    val SFTP_PWD= "xxxxxx"
    val filename = filenameRoot+day+".csv"
    var needUpdate = false
    var outCount = 0
    try {
      readCsvFromFTP(SFTP_HOST,SFTP_USER,SFTP_PWD,readFileFromFolder,filename,"true")
        .createOrReplaceTempView("outputRaw")
      outCount = spark.sql("select * from outputRaw limit 5").count().toInt
      //print("Existing output count: "+outCount.toString)
      if (outCount!=5) { needUpdate = true }
    } catch {
      case e: Exception => {
        println("Data of "+day+" not updated.")
        needUpdate = true
      }
    }
    println("Need update: "+needUpdate.toString)
    needUpdate
  }

  def isExistTable (day:String, tableName:String, dateColumn:String): Boolean = {
    spark.sql(s"select * from $tableName where $dateColumn='$day' limit 5").count().toInt==5
  }

  def saveResultToCsv(thisDay:String, saveTable:String, saveFileToFTPFolder:String, filenameRoot:String)={
    //val day = thisDay.replace("-","")
    val SFTP_HOST= "xxxxxx.com"
    val SFTP_USER = "xxxxx"
    val SFTP_PWD= "xxxxx"

    var saveFilename = ""
    val df = spark.sql("select * from " + saveTable)
      saveFilename = filenameRoot+"_"+thisDay+".csv"

    saveDfToFTP(df,SFTP_HOST,SFTP_USER,SFTP_PWD,saveFileToFTPFolder,saveFilename)

  }

  def saveDfToFTP(df:DataFrame,SFTP_HOST:String,SFTP_USER:String,SFTP_PWD:String,saveFileToFolder:String,fileName:String): Unit={
    df
      .coalesce(partitionNum)
      .write.format("com.springml.spark.sftp").
      option("host", SFTP_HOST).
      option("username", SFTP_USER).
      option("password", SFTP_PWD).
      option("fileType", "csv").
      option("delimiter", ",").
      option("codec", "bzip2").
      save(saveFileToFolder + fileName)
  }

  def dailyDropParquet(dayDrop:String, parquetPath:String): Unit = {
    try {
      val fs=FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val outPutPath=parquetPath
      if(fs.exists(new Path(outPutPath))) {
        fs.delete(new Path(outPutPath), true)
        println("Dropped parquet date: " + dayDrop)
//        println("Dropped "+parquetPath+" date: " + dayDrop)
      }
    } catch {
      case e: Exception => {
        println("Date not dropped: " + dayDrop)
      }
    }
  }

  def dailyDrop(dayDrop:String, tableDrop:String): Unit = {
    try {
      //println(s"alter table $tableDrop drop if exists partition (DATA_DATE=$dayDrop)")
      spark.sql(s"alter table $tableDrop drop if exists partition (data_date='$dayDrop')")
      println("Dropped table date: " + dayDrop)
//      println("Dropped table " + tableDrop + " date: " + dayDrop)
    } catch {
      case e: Exception => {
        println("Date not dropped: " + dayDrop)
        print(e)
      }
    }
  }

  def createDatabase(database:String): Unit = {
    spark.sql(s"create database if not exists $database")
  }


  def mergeFilesInHDFS(srcPath: String, dstPath: String): Unit =  {
    println ("Merging CSV")
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
  }

  def mergeFilesInHDFSWithHeader(srcPath: String, dstPath: String, header:String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    MyFileUtil.copyMergeWithHeader(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, header)
  }

  def isFileHDFS(filepath:String) : Boolean = {
    val fcheck = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val isFile = fcheck.exists(new Path(filepath))
    //println(filepath+" exists? "+isFile.toString)
    isFile
  }

  def isFileHDFSSizeGT100M(filepath:String) : Boolean = {
    //val filepath = "/xxx/xxx/xxx.parquet"
    val hdfs: org.apache.hadoop.fs.FileSystem =
      org.apache.hadoop.fs.FileSystem.get(
        new org.apache.hadoop.conf.Configuration())

    val hadoopPath= new org.apache.hadoop.fs.Path(filepath)
    val recursive = true
    val ri = hdfs.listFiles(hadoopPath, recursive)
    val it = new Iterator[org.apache.hadoop.fs.LocatedFileStatus]() {
      override def hasNext = ri.hasNext
      override def next() = ri.next()
    }

    // Materialize iterator
    val files = it.toList
    // println(files.size)
    //println(files.toString)
    //println(it.toString)
    files.map(_.getLen).sum > 256
//    files.map(_.getLen).sum > 1024*1024*100
  }

}

object letsGo{
  def main(args: Array[String]):Unit={
    val main_beg = System.nanoTime

    val spark = SparkSession
      .builder.appName("appName")
      .enableHiveSupport().getOrCreate()
    val partitionNum = 4608
    //val bucketNum = 128
    val bucketNum = 10

    spark.sparkContext.setLogLevel("ERROR")
//    spark.conf.set("spark.sql.shuffle.partitions",partitionNum.toString)
    //spark.conf.set("spark.network.timeout","800000")
    //spark.conf.set("spark.cores.max","128")
    //spark.conf.set("spark.sql.shuffle.partitions","22")
    //spark.conf.set("spark.scheduler.mode", "FAIR")
    //spark.conf.set("spark.shuffle.file.buffer","64k")
    //spark.conf.set("spark.reducer.maxSizeInFlight","96m")
    //spark.conf.set("spark.yarn.executor.memoryOverhead","600")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

//    println(args(0).toString)
//    println(args(1).toString)
    val isTest = if (Try(args(0).toString=="test").isSuccess) args(0).toString=="test" else false
    if(isTest) {println("TEST MODE")} else {println("REGULAR MODE")}

    //val dateFormat = new SimpleDateFormat("yyyyMMdd")
    //val dateFormatHyphen = new SimpleDateFormat("yyyy-MM-dd")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //val dateFormatDow = new SimpleDateFormat("u")

    var LOOKBACK = Array(1,2,3,4,5,6,7)
    try {
      val nDesignated = if(isTest) args(1).toInt else args(0).toInt
      LOOKBACK = Array(nDesignated)
      println("Checking "+nDesignated.toString+" days ago...")
      } catch {
        case e: Exception => {
          println("Checking default lookback period...")
        }
      }

    def computeMS(nLookbackTo:Int): Unit= {
      val cal = Calendar.getInstance
      cal.add(Calendar.DATE, -nLookbackTo)
      val day = dateFormat.format(cal.getTime)


      val ms = new mainClass(spark, partitionNum)

      //ms.createDatabase(database)

      val time_beg = System.nanoTime()

      val f = Future {
        try {
          println("Do something ...")
        } catch {
          case e: Exception => {
            println("(())")
          }
        }

        println("Elapsed time: " + ((System.nanoTime() - time_beg) / (1e+9)) + " s")

      }

    try {
        Await.result(f, 480 minutes);
      } catch {
        case e: TimeoutException => println(e)
      }
    }

    LOOKBACK.foreach(nLookbackTo => {
      computeMS(nLookbackTo)
    })


    spark.stop()

    val main_duration = (System.nanoTime - main_beg) / 1e9d
    println(main_duration)
  }
}