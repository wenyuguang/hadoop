package hadoop.spark.nginx;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class NginxLogParse {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("nginx日志解析").setMaster("spark://127.0.0.1:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 创建lines RDD  "hdfs://150.0.2.44:9000/log/nginxlog/log.log"
        JavaRDD<String> lines = sc.textFile(args[0]);

        // 将文本分割成单词RDD
        JavaRDD<ApacheAccessLog> words = lines.flatMap(new FlatMapFunction<String, ApacheAccessLog>() {
            /**
			 * 2018年9月5日下午3:14:43
			 * Tony
			 */
			private static final long serialVersionUID = 1L;

			@Override
            public Iterator<ApacheAccessLog> call(String s) throws Exception {
				try {
					String[] log = s.split(" ");
					if(log.length == 13) {
						String ipAddress = log[0];
						String dateTime = log[3].replace("[","");
						String method = log[5].replace("\"","");
						String endpoint = log[6];
						String protocol = log[7].replace("\"","");
						String responseCode = log[8];
						String contentSize = log[9];
						String clientIdentd = log[11].replace("\"","");
						return Arrays.asList(new ApacheAccessLog(ipAddress, dateTime, method, endpoint, protocol, responseCode, contentSize, clientIdentd)).iterator();
					}
				} catch (Exception e) {
					return Arrays.asList(new ApacheAccessLog()).iterator();
				}
				return null;
            }
        });
        
        JavaPairRDD<ApacheAccessLog, Integer> map = words.mapToPair(new PairFunction<ApacheAccessLog, ApacheAccessLog, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -978574386700107488L;

			@Override
			public Tuple2<ApacheAccessLog, Integer> call(ApacheAccessLog t) throws Exception {
				
				return new Tuple2<ApacheAccessLog,Integer>(t,1);
			}
        	
		});
        
        JavaPairRDD<ApacheAccessLog, Integer> count = map.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
        
        
        count.saveAsTextFile(args[1]);
        sc.close();
	}
}
