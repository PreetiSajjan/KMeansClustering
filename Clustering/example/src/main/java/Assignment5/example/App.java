package Assignment5.example;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

public class App 
{
    public static void main( String[] args )
    {
    	
    	System.setProperty("hadoop.home.dir", "C:/winutils");
		SparkConf sparkConf = new SparkConf()
				.setAppName("Twitter")
				.setMaster("local[4]").set("spark.executor.memory", "1g"); //4 core processor to work individually with 1 gigabyte of heap memory
		
		// Set the system properties so that Twitter4j library used by Twitter stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", "uxDvzBQtcTar6CWJTD7QKCuRd");
		System.setProperty("twitter4j.oauth.consumerSecret", "G2L7CIusnuzScCXUl1VLXUiTxk8PpA7qGofilRklS3QndrGDsF");
		System.setProperty("twitter4j.oauth.accessToken", "1195840617185722374-Ow6KOXD8u5YU11PliqbX69igsf83ZY");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "ENqiUAwYA9QSp76xxhuaBaznToZC2yBRToxJhWQLA07Y0");


		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
		
		
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);

		JavaDStream<String> words = stream.flatMap(new FlatMapFunction<Status, String>() {
			
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(Status s) {
				return Arrays.asList(s.getText().split(" ")).iterator();
			}
		});

		JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(String word) {
				return word.startsWith("#");
			}
		});

		JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(
				new PairFunction<String, String, Integer>() {
					
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String s) {
						// leave out the # character
						return new Tuple2<String, Integer>(s.substring(1), 1);
					}
				});

		hashTagCount.print();
		/*JavaPairDStream<String, Integer> hashTagTotals = hashTagCount.reduceByKeyAndWindow(
				new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				}, new Duration(10000));*/
		
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    	
    }
}
