package PageRank;

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
import java.io.Serializable;

public class PageRank {
	private static int k;
	
	private static class myComp implements Comparator<Tuple2<Integer, Double>>, Serializable {

		@Override
		public int compare(Tuple2<Integer, Double> o1,
				Tuple2<Integer, Double> o2) {
			return -1 * (Double.compare(o1._2(), o2._2()));
		}
		
	}
	private static class buildRank extends PairFunction<String, Integer, Double> {
		private int n;
		public buildRank (int _n) {
			n = _n;
		}
		@Override
		public Tuple2<Integer, Double> call(String arg0)
				throws Exception {
			String[] strings = arg0.split(" ");
			return new Tuple2<Integer, Double> (Integer.parseInt(strings[0]), 1.0 / n);   //need to modify
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
	public static void main(String[] args) {
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
		for (int i = 0; i < k; i++) {
			rank.add(i, ctx.textFile("/user/zzq/data/vector_" + String.valueOf(i+1)).map(new buildRank(n)));
		}
		
		ArrayList<ArrayList<JavaPairRDD<Integer, List<Integer>>>> matrixs = new ArrayList<ArrayList<JavaPairRDD<Integer, List<Integer>>>>(k);
		for (int i = 0; i < k; i++) {
			matrixs.add(i, new ArrayList<JavaPairRDD<Integer, List<Integer>>>(k));
			for (int j = 0; j < k; j++) {
				matrixs.get(i).add(j, ctx.textFile("/user/zzq/smallMs/SmallMatrix_" + String.valueOf(i+1) + "_" + String.valueOf(j+1)).map(
						new PairFunction<String, Integer, List<Integer>>(){
							@Override
							public Tuple2<Integer, List<Integer>> call(
									String arg0) throws Exception {
								// TODO Auto-generated method stub
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
				for (int i = 1; i < k; i++) {
					contribs.get(0).set(j, contribs.get(0).get(j).union(contribs.get(i).get(j)));
				}
				rank.set(j, contribs.get(0).get(j).reduceByKey(new sum()).mapValues(new updateRank(n, beta)));
			}
		}
		
		for (int i = 1; i < k; i++) {
			rank.set(0, rank.get(i).union(rank.get(0)));
		}
		
		int top_num = (n > 100? 100 : 4);
		List<Tuple2<Integer, Double>> output = rank.get(0).takeOrdered(top_num, new myComp());
		for (Tuple2<?, ?> tuple: output) {
			System.out.println(tuple._1() + "___"  +tuple._2());
		}
				
		System.exit(0);
	}

}
