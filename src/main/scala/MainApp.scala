import java.net.InetAddress

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by sarathchandra on 7/27/2015.
 */

object MainApp {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","D:\\winutil\\")
    val sparkConf=new SparkConf()
      .setAppName("SparkApp")
      .set("spark.executor.memory", "4g").setMaster("local[*]")
    val ssc= new StreamingContext(sparkConf,Seconds(2))
    val sc=ssc.sparkContext

    /*----to be removed----*/
    calorieApi.findCalorie(sc,"vegetable burger")
    Recommendation.foodRecommend(sc,"5","male")
    val sentimentAnalyzer: SentimentAnalyzer = new SentimentAnalyzer
    val tweetWithSentiment: TweetWithSentiment = sentimentAnalyzer.findSentiment("Food is delicious")
    System.out.println(tweetWithSentiment.toString().replaceAll("sentiment : ",""))
    /*--------------------------*/

    var age:String = ""
    val ip=InetAddress.getByName("10.205.0.63").getHostName
    val lines=ssc.socketTextStream(ip,1234)

    val command= lines.map(x=>{
      val y=x.toUpperCase()
      y
    })
    command.foreachRDD(
      rdd=> {
        if (rdd.collect().contains("RECOMMEND:::age")) {
          System.out.println("Got Recommend command")
          //Recommendation.recommend(rdd.context)
           age = rdd.collect().mkString("").replace("RECOMMEND:::age","")
          System.out.println(age)

        }
        else if (rdd.collect().contains("RECOMMEND:::gender")) {
          System.out.println("Got Recommend command")
          //Recommendation.recommend(rdd.context)
          val gender = rdd.collect().mkString("").replace("RECOMMEND:::gender","")
          System.out.println(gender)
          Recommendation.foodRecommend(sc,age,gender)

        }
        else if (rdd.collect().contains("SENTIMENT")) {
          val sentimentAnalyzer: SentimentAnalyzer = new SentimentAnalyzer
          val tweetWithSentiment: TweetWithSentiment = sentimentAnalyzer.findSentiment("click here for your Sachin Tendulkar personalized digital autograph.")
          System.out.println(tweetWithSentiment.toString().replaceAll("sentiment : ",""))
          iOSConnector.sendCommandToRobot("sentiment:::"+tweetWithSentiment.toString().replaceAll("sentiment : ",""))

        }
        else if (rdd.collect().contains("PARSE")) {
          val input = rdd.collect().toString().replaceAll("PARSE:","")
          calorieApi.findCalorie(sc,input)
        }
      }
    )
    lines.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
