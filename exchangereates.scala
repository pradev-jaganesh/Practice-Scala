package idw.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.amazonaws.auth._
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.redshift.AmazonRedshiftClient 
import _root_.com.amazon.redshift.jdbc41.Driver
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import scala.collection.Map
import org.apache.log4j.Logger
import org.apache.log4j.Level
import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.sql.functions.udf
//import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.amazonaws.services.s3._,model._
import com.amazonaws.services.s3.AmazonS3Client
import com.typesafe.config.{ Config, ConfigFactory }
import com.amazonaws ._ 
import com.amazonaws.auth ._ 
import com.amazonaws.services.s3 ._ 
import com.amazonaws. services.s3.model ._ 
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
//import sqlContext.implicits._
import org.apache.spark.sql.functions.{unix_timestamp,to_date}

object exchangereates extends App {
  val provider = new InstanceProfileCredentialsProvider();
val credentials: AWSSessionCredentials = provider.getCredentials.asInstanceOf[AWSSessionCredentials];
val token = credentials.getSessionToken;
val awsAccessKey = credentials.getAWSAccessKeyId;
val awsSecretKey = credentials.getAWSSecretKey
val conf = new SparkConf().setAppName("indexvaluesActuaries").set("spark.sql.crossJoin.enabled", "true")//.setMaster("local[*]")
Logger.getLogger("org").setLevel(Level.OFF)
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)//.read.format("com.datadricks.spark.csv").option("delimiter","\t")

import sqlContext.implicits._
import java.util.zip.CRC32
import java.util.Arrays
import org.apache.spark.sql.functions.udf
System.setProperty("spark.sql.shuffle.partitions","8")
System.setProperty("spark.sql.tungsten.enabled","false")
// val s3TempDir1 = "s3a://d-idw-bo-input-s3/idwdevftse/sparkfromide/nonEq/ftseconsreview/"
 //val tempS3Dir = "s3a://d-idw-bo-input-s3/idwdevftse/spark-temp/closeHoldings"


val config = ConfigFactory.parseFile(new File("E:\\Users\\Abirami\\Downloads\\spark-sit.conf"))

val inputfileSchema = Array("Date","ISO_Currency_Code","USD_Exchange_Rate")

//SparkSession.builder().config("spark.sql.warehouse.dir", "file:///C:/Users/Abirami/workspace/Sample/spark-warehouse").getOrCreate()
//System.setProperty("hadoop.home.dir", "E:\\hadoop\\bin\\winutils.exe")
System.setProperty("spark.files.overwrite","true")
System.setProperty("spark.dynamicAllocation.enabled","true")
//val jdbcURL = config.getString("idwsitconfig.redshift.url")
//val jdbcURL = "jdbc:redshift://dev-idw-redshiftcluster-071918.cztizil2d3ce.us-east-1.redshift.amazonaws.com:5439/devidwredshiftcluster?user=admin&password=Admin123"
//val jdbcURL = "jdbc:redshift://sit-idw-redshiftcluster.cpkphyncp4u0.us-west-2.redshift.amazonaws.com:5439/sitidwredshiftcluster?user=admin&password=Admin5439"
//val tempS3DirIndex = "s3a://d-idw-bo-input-s3/idwdevftse/idwdevftse/spark-temp/DIM_INDEX"
println("Before File read")
 //************Deleting the older file in HDFS**********************    
val fs=FileSystem.get(sc.hadoopConfiguration)
val configBucketName = args(0)
val conprefix = args(1)
//val outPutPath="hdfs:///user/hadoop/exchangerates/exchangerates.csv"

/*
if(fs.exists(new Path(outPutPath)))
  fs.delete(new Path(outPutPath),true)
   println("After File read")
 val urlPrefix = "https://301068310399.signin.aws.amazon.com/console"
 val bucketName = "d-idw-bo-input-s3"
 val prefix = "idwdevspark/inputfiles/nonequity/exchangerates/"
 */              

 var listObjectsRequestO = new ListObjectsRequest()
          listObjectsRequestO.withBucketName(configBucketName)
          listObjectsRequestO.withPrefix(conprefix)
    val client = new AmazonS3Client (credentials)
    val obsO = client.listObjects(listObjectsRequestO);
    val objectListO = obsO.getObjectSummaries();
    var itr1=0;
    val confpath ="s3a://"+configBucketName+"/"+ objectListO.get(itr1).getKey
    println(confpath)
    val conffile = sc.textFile(confpath).filter(!_.isEmpty())   
    val keyvaluepair =  conffile.map(line => line.split("~").map(_.trim)).map(r => (r(0),r(1))).collectAsMap().toMap 
    val jdbcURL = keyvaluepair.get("redshift_endpoint").mkString
    val jdbcURLPostgres = keyvaluepair.get("aurora_endpoint").mkString       
    val urlPrefix = keyvaluepair.get("urlPrefix").mkString
    val prefix = keyvaluepair.get("exchangerates_input_files").mkString
    val outputPath=keyvaluepair.get("exchangerates_outputPath").mkString
    val bucketName = keyvaluepair.get("s3_bucket").mkString
    val redshift_schema=keyvaluepair.get("redshift_schema_nonequity").mkString
    val aurora_schema=keyvaluepair.get("aurora_schema").mkString
	
 //************Deleting the older file in HDFS**********************    


//val outPutPath="file:///C:/Users/Abirami/Desktop/ftseconscurr.csv"

if(fs.exists(new Path(outputPath)))
  fs.delete(new Path(outputPath),true)
  println("After list read")
  
  var listObjectsRequest = new ListObjectsRequest()
        listObjectsRequest.withBucketName(bucketName)
        listObjectsRequest.withPrefix(prefix)
    val obs = client.listObjects(listObjectsRequest);
    val objectList = obs.getObjectSummaries(); 
  
  var itr=0;
  for (itr <- 1 until objectList.size()){
      val path = "s3a://"+bucketName+"/"+(objectList.get(itr).getKey)
      var tempfilename = objectList.get(itr).getKey.toString()
      
      tempfilename=tempfilename.substring(tempfilename.lastIndexOf("/")+1, tempfilename.length()).replace("[","").replace("]","")
      
     //var Descfilename=Descfilename.substring(Descfilename,0,4)
     
     //var Descfilename= substring(tempfilename,0, 10)
      val Lfname=tempfilename.length()
      val Sfname=Lfname-8
      //val Nfname=Sfname+1
      val Descfilename= tempfilename.substring(0,Sfname)//  substring,0, 4)
      println("sub filname",Descfilename)
     
       val getfilenameDesc = List(Descfilename)
    val fileNameDescDF = sc.parallelize(Seq(Descfilename)).toDF("filenameDesc")
   
    println("filename description"+ Descfilename)
     // var FilesubString :DataFrame = sc.parallelize(Descfilename)//.toDF("filedisc")
    //val DescF : DataFrame=sc.parallelize(tempfilename)
      
      println("file name "+tempfilename)//printing file name
    
    ///println(path)
  val matchingLineAndLineNumberTuples = sc.textFile(path).zipWithIndex()
   .filter({
  case (line, lineNumber) => line.contains("XXXXXXXXXX")
  }).collect
  //println("index test")
  val index = matchingLineAndLineNumberTuples.map{case(line, number) => number}
      //println("length :" +index(0))
  var tempinputfile = sc.textFile(path).take(index(0).toInt)
  var inputfile = sc.parallelize(tempinputfile)
  /*Take Description from 2 line */
  
  val desc = inputfile.take(2).takeRight(1)
  var descfromFile : DataFrame =sc.parallelize(desc).toDF("Description")
  println("printing description")
  //descfromFile
  descfromFile.show()
  
    /* Take the first 3 non empty lines as the header */
  val header = inputfile.filter(!_.isEmpty()).take(3)
  
  /* Get the date from the file */
      //  val getDate= inputfile.take(1)//.substring(inputfile,0, 10)//inputfile.take(1)       //tempfilename.substring(0,Sfname)//  substring,0, 4)

  
  var getDate = inputfile.take(1)
        .map(x => x.split(' '))
        .map(x => x(0))
        .map(x => x.split("/"))
        .map(x => x(2) + '-' + x(1) + '-' + x(0).mkString)
        if (tempfilename.substring(0, tempfilename.length()-8) == "wixr")
        {
           println(getDate(0).substring(0,4).concat(getDate(0).substring(7,13)))
           getDate(0)=getDate(0).substring(0,4).concat(getDate(0).substring(7,13))
        }
        var dateFromFile :DataFrame = sc.parallelize(getDate).toDF("asOfDate") 
        
        /* Strip off the header from the file */
    inputfile = inputfile.mapPartitionsWithIndex{ (idx, iter) => if (idx == 0 ) iter.drop(4) else {iter}}//filter(!_.isEmpty())
    
    inputfile = inputfile.filter(!_.isEmpty()).filter(row => row != header(2)) 
   //inputfile = sc.parallelize(inputfile.toDF().columns.dropRight(1))
        
    var fileDF = inputfile.toDF()
    fileDF = fileDF.withColumn("temp", split(col("value"), "\\,")).select(
    (0 until 3).map(i => col("temp").getItem(i).as(inputfileSchema(i))): _*)
 
    
    val getfilename = List(tempfilename)
    val fileNameDF = sc.parallelize(Seq(tempfilename)).toDF("filename")
    
    /* substing the filename*/
    

   var inputFiles = fileDF.as('df1).join(dateFromFile.as('df2)).join(fileNameDF.as('df3)).join(descfromFile).join(fileNameDescDF.as('df4))
   
   inputFiles.show()
    inputFiles.coalesce(1).write.option("delimiter",",")
    .mode("append").option("header", "false")// .save("file:///C:/Users/Abiram/Desktop/ftseconscurr.csv")
  .save(outputPath)
  println("after write")
    //.save("E:\\Users\\vpremasa\\Desktop\\exchange.csv")
      
    //inputFiles.repartition(8).coalesce(8).write.option("delimiter","|").mode("append").option("header", "false")
    //.save("E:/Users/Abirami//Desktop/ftseconscurr.csv") 
  inputFiles.show()
  println("afterwrite")
  }
  }