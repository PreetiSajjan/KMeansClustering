import java.io.File;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

/**
 * Implementing KMeanClustering algorithm to 
 * cluster given Twitter tweets by their geographic origins (coordinates)
 * @author Preeti Sajjan
 * 
 */
public class KMeansCluster {

	public static void main(String args[]) {
		
		System.setProperty("hadoop.home.dir", "C:/winutils");
		SparkConf sparkConf = new SparkConf()
				.setAppName("K-means Example (from Apache Spark distribution)")
				.setMaster("local[4]").set("spark.executor.memory", "1g"); // 4 core processor to work individually with 1 gigabyte of heap memory
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// relative path
		File textfile = new File("twitter2D.txt");
				
		// absolute path to our data file
		String path = textfile.getAbsolutePath();
		// reading the text file
		JavaRDD<String> data = sc.textFile(path);
		
		// each line in file is split by ","
		// and returning a tuple of vector consisting of world coordinates and string containing Twitter tweets
		JavaRDD<Tuple2<Vector, String>> parsedData = data.map(line -> {
					String[] sarray = line.split(",");					
					
					double[] values = new double[2];
					for (int i = 0; i < 2; i++)
						values[i] = Double.parseDouble(sarray[i]);
					
					return new Tuple2<Vector, String>(Vectors.dense(values), sarray[sarray.length-1]);
					}
				);		
		parsedData.cache();
		
		// Cluster the data into four classes using KMeans
		int numClusters = 4;
		int numIterations = 20;
		
		// Training the KMeans algorithm with vector of world coordinates
		KMeansModel model = KMeans.train(parsedData.map(p -> p._1).rdd(), numClusters, numIterations);
		
		// Predicting the cluster index to each tweet and returning a
		// list of tuple with string containing tweet with integer containing the cluster index to which the tweet belongs to
		List<Tuple2<String, Integer>> prediction = parsedData.map(line ->{
			int cluster = model.predict(line._1);
			return new Tuple2<String, Integer>(line._2, cluster);
		}).sortBy(x -> x._2, true, 1).collect(); 	// sorting the list by cluster index
		
		// Printing the results
		System.out.println("\n");
		for (Tuple2<String, Integer> element : prediction) {
			System.out.println("Tweet \"" + element._1 + "\" is in cluster " + element._2);
		}
		System.out.println("\n");
		
		// Closing the resource
		sc.close();
	}
}