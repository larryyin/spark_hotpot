package com.larryyin.spark.hotpot

import org.apache.spark.storage.StorageLevel
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
//import org.datasyslab.geospark.formatMapper.GeoJsonReader
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
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
//import com.vividsolutions.jts.geom.Geometry
//import org.datasyslab.geospark.enums.FileDataSplitter

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

  def Func1(day:String,nLookback:Int, outDir:String, dataDir:String, outColumns:Array[String], outColumnsTmp:Array[String], partitionNum:Int): Unit = {
    //    val dates_str = (0 to nLookback-1).map(x => DateAdd(day,-x)).mkString(",").replaceAll("-","")
//    println("Job starts...")
    val dates = ListBuffer[String]()
    var dateTmp = DateAdd(day, -nLookback - 1)
    //    val fs=FileSystem.get(spark.sparkContext.hadoopConfiguration)
    while (dateTmp!=day) {
      dateTmp = DateAdd(dateTmp,1)
      val datePath=dataDir+dateTmp+".parquet"
      if(isFileHDFS(datePath) && isFileHDFSSizeGT100M(datePath)) {
        dates += dateTmp
      }
    }
    println(dates.mkString(","))
    val hdSchema = spark.read.parquet(dataDir+day+".parquet").schema
    var MC = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], hdSchema)
    for (date <- dates) {
      MC = MC.union(spark.read.parquet(dataDir + date + ".parquet"))
    }


  def DateAdd(day:String, dDay:Int):String = {
    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(fmt.parse(day))
    cal.add(Calendar.DATE, dDay)
    fmt.format(cal.getTime)
  }

  def isExistTable (day:String, tableName:String, dateColumn:String): Boolean = {
    spark.sql(s"select * from $tableName where $dateColumn='$day' limit 5").count().toInt==5
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

  /*  def createDatabase(database:String): Unit = {
      spark.sql(s"create database if not exists $database")
    }*/




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
    files.map(_.getLen).sum > 1024
    //    files.map(_.getLen).sum > 1024*1024*100
  }

}

object letsGo {
  def main(args: Array[String]): Unit = {
    val main_beg = System.nanoTime

    val spark = SparkSession
      .builder.appName("geospark_template")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator")
      .enableHiveSupport().getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    GeoSparkSQLRegistrator.registerAll(spark)


    val zipcodeTargets = spark.read.option("header", "true").csv("hdfs://user@server:9001/xxx/abc.csv").select("zipcode")

    spark
      .read.format("csv").option("delimiter", "\t").option("header", "false")
      .load(parsedGeoJsonFilePath)
      .withColumn("zipcode", split(split(col("_c0"), "ZCTA5CE10\":\"")(1), "\"},\"type")(0))
      .join(broadcast(zipcodeTargets), Seq("zipcode"), "inner")
      .dropDuplicates("zipcode")
      .withColumn("geometry", split(split(col("_c0"), "geometry\":")(1), ",\"properties")(0))
      .withColumn("geometry", concat(
        split(col("geometry"), "\\[\\[\\[")(0),
        lit("[[["),
        regexp_replace(split(split(col("geometry"), "\\[\\[\\[")(1), "\\]\\]\\]")(0), lit("\""), lit("")),
        lit("]]]"),
        split(col("geometry"), "\\]\\]\\]")(1)
      ))
      .select("zipcode", "geometry")
      .filter("cast(zipcode as int) is not null")
      .dropDuplicates("zipcode")
      .repartition(3)
      .createOrReplaceTempView("geoTextTable")

    val zip_polygon = spark.sql("select zipcode, ST_GeomFromGeoJSON(geometry) as geo from geoTextTable").persist(StorageLevel.MEMORY_ONLY)

    zip_polygon.show(2)
    zip_polygon.createOrReplaceTempView("zip_polygon")

      val points = spark.sql("SELECT *,ST_Point(CAST(dff.LONGITUDE AS Decimal(10,6)), CAST(dff.LATITUDE AS Decimal(10,6))) AS pt FROM dff")
      //    points.show(5,false)
      points.createOrReplaceTempView("points")

      //    spark.sql("select * from points").show(5, false)

      spark.sql("SELECT /*+ BROADCAST(zip_polygon) */ * FROM points, zip_polygon WHERE ST_Within(points.pt, zip_polygon.geo)")
        .drop("pt")
        .drop("geo")
        .dropDuplicates("zipcode")
        .repartition(30)
        .write.mode("overwrite").option("compression", "gzip").parquet(outName)

      spark.read.parquet(outName).show(2,false)

      println(outName)
      println((System.nanoTime - dayhr_beg) / 1e9d)
    }

    (args(0).toInt to args(1).toInt).foreach(day_num =>
      (0 to 23).foreach(hr_num =>
        computeDay(day_num, hr_num, cols, inputPathRoot, outPathRoot)
      )
    )


    spark.stop()


    val main_duration = (System.nanoTime - main_beg) / 1e9d
    println(main_duration)
  }
}
