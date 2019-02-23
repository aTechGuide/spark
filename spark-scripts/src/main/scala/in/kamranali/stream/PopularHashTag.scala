package in.kamranali.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PopularHashTag {

  def setUpTwitter(): Unit = {
    import scala.io.Source

    for (line <- Source.fromFile("./src/main/resources/stream/twitter.txt").getLines()) {
      val fields = line.split(" ")
      if (fields.length == 2)
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
    }
  }

  def main(args: Array[String]): Unit = {

    // Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)

    setUpTwitter()

    val ssc = new StreamingContext("local[*]", "PopularHashTagApp", Seconds(1))

    // Creating a DStream from Twitter using Streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    val hashTags = tweets.map(status => status.getText).flatMap(tweetText => tweetText.split(" ")).filter(word => word.startsWith("#"))

    val hashTagKeyValues = hashTags.map(hashtag => (hashtag, 1))

    // Combining all information in DStream over a 5 min window sliding every one second
    val hashTagCounts = hashTagKeyValues.reduceByKeyAndWindow((x,y) => x+y, (x,y) => x-y, Seconds(300), Seconds(1))

    val sortedResults = hashTagCounts.transform(rdd => rdd.sortBy(x => x._2, false))

    sortedResults.print

    ssc.checkpoint("/Users/kamali/temp")
    ssc.start()
    ssc.awaitTermination()


  }

}
