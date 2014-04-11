package PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.io.IOException;
import java.io.Serializable;

public class PageRank {
	private static int k;
	
	private static class myComp implements Comparator<Tuple2<String, Double>>, Serializable {

		@Override
		public int compare(Tuple2<String, Double> o1,
				Tuple2<String, Double> o2) {
			return -1 * (Double.compare(o1._2(), o2._2()));
		}
		
	}
	private static class readPage extends PairFunction<String, Integer, String> {
		private int n;
		public readPage (int _n) {
			n = _n;
		}
		@Override
		public Tuple2<Integer, String> call(String arg0)
				throws Exception {
			if (arg0.length() == 0) return new Tuple2<Integer, String> (0, "nothing");
			String[] strings = arg0.split(" ");
			return new Tuple2<Integer, String> (Integer.parseInt(strings[0]), strings[1]);   //need to modify
		}
	}
	
	private static class buildRank extends Function<String, Double>{
		private int n;
		public buildRank (int _n) {
			n = _n;
		}
		@Override
		public Double call(String arg0) throws Exception {
			// TODO Auto-generated method stub
			return new Double(1.0 / n);
		}
		
	}
	private static class sum extends Function2<Double, Double, Double> {

		@Override
		public Double call(Double arg0, Double arg1) throws Exception {
			return arg0 + arg1;
		}
		
	}
	
	private static class updateRank extends Function<Double, Double> {
		private int n;
		private double beta;
		public updateRank(int _n, double _beta){
			n = _n;
			beta = _beta;
		}
		
		@Override
		public Double call(Double arg0) throws Exception {
			return arg0 * beta + (1-beta) / n;
		}
	}
	public static void main(String[] args) throws IOException {
		if (args.length != 4) {
			System.out.println("Usage: PageRank <URL number> <para> <iterate time> <matrixs divide>");
			System.exit(1);
		}
		int n = Integer.parseInt(args[0]);
		double beta = Double.parseDouble(args[1]);
		int iterate_time = Integer.parseInt(args[2]);
		k = Integer.parseInt(args[3]);
		
		JavaSparkContext ctx = new JavaSparkContext("yarn-standalone", "PageRank", 
				System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(PageRank.class));
		
		ArrayList<JavaPairRDD<Integer, Double>> rank = new ArrayList<JavaPairRDD<Integer, Double>>(k);
		ArrayList<JavaPairRDD<Integer, String>> page = new ArrayList<JavaPairRDD<Integer, String>>(k);
		for (int i = 0; i < k; i++) {
			page.add(i, ctx.textFile("/user/zzq/data/vector_" + String.valueOf(i+1)).map(new readPage(n)));
			rank.add(i, page.get(i).mapValues(new buildRank(n)));
		}
		
		ArrayList<ArrayList<JavaPairRDD<Integer, List<Integer>>>> matrixs = new ArrayList<ArrayList<JavaPairRDD<Integer, List<Integer>>>>(k);
		for (int i = 0; i < k; i++) {
			matrixs.add(i, new ArrayList<JavaPairRDD<Integer, List<Integer>>>(k));
			for (int j = 0; j < k; j++) {
				matrixs.get(i).add(j, ctx.textFile("/user/zzq/matrixs/SmallMatrix_" + String.valueOf(i+1) + "_" + String.valueOf(j+1)).map(
						new PairFunction<String, Integer, List<Integer>>(){
							@Override
							public Tuple2<Integer, List<Integer>> call(
									String arg0) throws Exception {
								// TODO Auto-generated method stub
								if (arg0.length() == 0) return null;
								String[] strings = arg0.split(" ");
								ArrayList<Integer> value = new ArrayList<Integer>();
								for (int i = 1; i < strings.length; i++) {
									value.add(Integer.valueOf(strings[i]));
								}
								return new Tuple2<Integer, List<Integer>>(Integer.parseInt(strings[0]), value);
							}
						}).cache());
				
			}
		}
		
		for (int i = 1; i < k; i++) {
			page.set(0, page.get(i).union(page.get(0)));
		}
		page.get(0).cache();
		
		int top_num = (n > 100? 100 : 4);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		for (int count = 0; count < iterate_time; count++ ) {
			ArrayList<ArrayList<JavaPairRDD<Integer, Double>>> contribs = new ArrayList<ArrayList<JavaPairRDD<Integer, Double>>>(k);
			for (int i = 0; i < k; i++) {
				contribs.add(i, new ArrayList<JavaPairRDD<Integer, Double>>(k));
				for (int j = 0; j < k; j++) {
					contribs.get(i).add(j, matrixs.get(i).get(j).join(rank.get(i)).values().flatMap(
							new PairFlatMapFunction<Tuple2<List<Integer>, Double>, Integer, Double>() {

								@Override
								public Iterable<Tuple2<Integer, Double>> call(
										Tuple2<List<Integer>, Double> arg0)
										throws Exception {
									ArrayList<Tuple2<Integer, Double>> ret = new ArrayList<Tuple2<Integer, Double>>();
									int n = arg0._1().get(0);
									for (int i = 1; i < arg0._1().size(); i++) {
										ret.add(new Tuple2(arg0._1().get(i), arg0._2()/n));
									}
									return ret;
								}
							}));
				}
			}
			
			for (int j = 0; j < k; j++) {
				JavaPairRDD<Integer, Double> tmp = contribs.get(0).get(j);
				for (int i = 1; i < k; i++) {
					tmp = tmp.union(contribs.get(i).get(j));
				}
				rank.set(j, tmp.reduceByKey(new sum()).mapValues(new updateRank(n, beta)));
			}
			
			JavaPairRDD<Integer, Double> allRank = rank.get(0);
			for (int i = 1; i < k; i++) {
				allRank = allRank.union(rank.get(i));
			}
			
			List<Tuple2<String, Double>> output = page.get(0).join(allRank).values().takeOrdered(top_num, new myComp());
			Path dir = new Path("/user/zzq/output_" + args[1] + "_" + String.valueOf(count));
			FSDataOutputStream outFile = fs.create(dir, true);
			for (Tuple2<String, Double> tuple: output) {
				outFile.writeBytes(tuple._1() + "___"  + tuple._2().toString() + "\n");
			}
			outFile.close();
			
		}
//		
//		for (int i = 1; i < k; i++) {
//			rank.set(0, rank.get(i).union(rank.get(0)));
//			page.set(0, page.get(i).union(page.get(0)));
//		}
		
//		List<Tuple2<String, Double>> output = page.get(0).join(rank.get(0)).values().takeOrdered(top_num, new myComp());
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
//		Path dir = new Path("/user/zzq/output");
//		FSDataOutputStream outFile = fs.create(dir, true);
//		for (Tuple2<String, Double> tuple: output) {
//			outFile.writeBytes(tuple._1() + "___"  + tuple._2().toString() + "\n");
//		}
//		outFile.close();
		
		System.exit(0);
	}

}
