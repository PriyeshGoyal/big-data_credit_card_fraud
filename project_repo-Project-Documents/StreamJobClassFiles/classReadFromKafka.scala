
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType

object classReadFromKafka extends App{
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark=SparkSession.builder().master("local[2]").appName("streamingApp").getOrCreate();
  
  val tranxns=spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "cctxnstopic")
  .option("startingOffsets", "latest")
  .load()

  
 val tranxnsString = tranxns.selectExpr("CAST(value AS STRING)","timestamp")
 
 val schema = new StructType()
      .add("card_id",LongType)
      .add("member_id",LongType)
      .add("amount",DoubleType)
      .add("pos_id",IntegerType)
      .add("post_code",IntegerType)
      .add("transc_dt",StringType)
      //.add("status",StringType)
      
    val txnDF = tranxnsString.select(from_json(col("value"), schema).alias("value"),col("timestamp")).select("value.*","timestamp")
   
    //val tranxnsDF2 = tranxnsDF.withColumn("status", (col("member_id") +','+ col("amount").toString()))
    
    val txnDF2 = txnDF.createOrReplaceTempView("CC_TXNS")
    
    val txnDF3 = spark.sql("select concat(card_id,',',member_id,',',amount,',',pos_id,',',post_code,',',transc_dt) all_col_values from CC_TXNS a")
    
    
  
  //myObj.CardValidation(col("all_col_values").toString())
  
    val txnDF4 = txnDF3.withColumn("status", col("all_col_values"))
    
    //val myRdd = txnDF3.rdd
    
    //|        card_id|        member_id|  amount|pos_id|post_code| transc_dt|           timestamp
    
    
    txnDF4.writeStream.foreachBatch{ (batchDf : DataFrame, batchId : Long)=>{
      batchDf.foreach{ myRow => {
      val myObj = new classCardValidation();
      println("BatchID :"+ batchId)
      var myArray = myRow.mkString(",").split(",")
      println("Arry is created" )
      myObj.CardValidation(myArray)   
      println("Success :"+ batchId)
      }
      }
    }     
    }.start().awaitTermination()
  
     /*txnDF4.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
      * 
      */
      
      
    
    
    
    
    /*foreach { myRow =>
	  
  var myArray = myRow.mkString(",").split(",") 
  
  
  
    
    var cardIdVal = myArray(0)
    var memberIdVal = myArray(1)
    var txnAmountVal = myArray(2)
    var posIdVal = myArray(3)
    var postCodeVal = myArray(4)
    var currTxnTimeVal = myArray(5)*/

    //val myObj = new classCardValidation()
    //val myVal  = Array("22222","22222","50000000","1212","10001","2021-02-01 19:19:41")
    //myObj.CardValidation(myArray)    
    
}


/*
{"card_id": 917221245657777,"member_id": 37495064475648584,"amount": 9000.567,"pos_id": 1111,"post_code":10001,"transc_dt":"2021-01-01 17:19:41"}
{"card_id": 917221245657777,"member_id": 37495064475648584,"amount": 9000.567,"pos_id": 1111,"post_code":10001,"transc_dt":"2021-01-01 17:19:41"}
{"card_id": 917221245657777,"member_id": 37495064475648584,"amount": 9000.567,"pos_id": 1111,"post_code":10001,"transc_dt":"2021-01-01 17:19:41"}
{"card_id": 917221245657777,"member_id": 37495064475648584,"amount": 9000.567,"pos_id": 1111,"post_code":10001,"transc_dt":"2021-01-01 17:19:41"}

{"card_id": 917221245657778,"member_id": 37495064475648584,"amount": 9000.567,"pos_id": 2222,"post_code":10001,"transc_dt":"2021-01-01 18:19:41"}
{"card_id": 917221245657757,"member_id": 37495064475648584,"amount": 9000000.567,"pos_id": 3333,"post_code":10301,"transc_dt":"2021-01-01 19:19:41"}
{"card_id": 917221245657757,"member_id": 37495064475648584,"amount": 9000.567,"pos_id": 4444,"post_code":10301,"transc_dt":"2021-01-01 20:19:41"}
* 

*/

//./kafka-server-start.sh ../config/server.properties
//./kafka-topics.sh  --create --topic cctxnstopic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

//./kafka-console-producer.sh --broker-list localhost:9092 --topic cctxnstopic

//./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cctxnstopic --from-beginning

/*object classReadFromKafka  extends App {
  
}
*/