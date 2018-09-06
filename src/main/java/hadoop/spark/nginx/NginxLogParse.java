package hadoop.spark.nginx;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class NginxLogParse {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("nginx日志解析").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        
        // 创建lines RDD  "hdfs://150.0.2.44:9000/log/nginxlog/log.log"
        JavaRDD<String> lines = sc.textFile("d://access_log.2018-08-10.log");
        //RDD转DataFrame
        JavaRDD<ApacheAccessLog> apacheAccessLog = lines.map(new Function<String, ApacheAccessLog>() {
        	/**
			 * 2018年9月6日下午4:20:23
			 * Tony
			 */
			private static final long serialVersionUID = 1L;

			@Override
        	public ApacheAccessLog call(String v1) throws Exception {
				String[] log = v1.split(" ");
				if(log.length == 13||log.length == 12) {
					String ipAddress = log[0];
					String dateTime = log[3].replace("[","");
					String method = log[5].replace("\"","");
					String endpoint = log[6];
					String protocol = log[7].replace("\"","");
					String responseCode = log[8];
					String contentSize = log[9];
					String clientIdentd = log[11].replace("\"","");
					ApacheAccessLog apacheAccessLog = new ApacheAccessLog(ipAddress, dateTime, method, endpoint, protocol, responseCode, contentSize, clientIdentd);
					return apacheAccessLog;
				}
				return new ApacheAccessLog();
        	}
		}).filter(new Function<ApacheAccessLog, Boolean>() {
			/**
			 * 2018年9月6日下午5:31:42
			 * Tony
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(ApacheAccessLog v1) throws Exception {
				return v1.getIpAddress() == null;
			}
		});
        
        
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));

        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);

        
        
        
        
        
        
        Dataset<Row> df = sqlContext.createDataFrame(apacheAccessLog, ApacheAccessLog.class);
        df.show();
        df.createOrReplaceTempView("log");
        sqlContext.sql("select * from log").show();
        
        JavaRDD<Row> javaRDD = df.javaRDD();
        JavaRDD<ApacheAccessLog> map = javaRDD.map(new Function<Row, ApacheAccessLog>(){
        	/**
			 * 2018年9月6日下午4:50:05
			 * Tony
			 */
			private static final long serialVersionUID = 1L;

			@Override
        	public ApacheAccessLog call(Row v1) throws Exception {
				return new ApacheAccessLog((String)v1.getAs("ipAddress"), 
						(String)v1.getAs("dateTime"), 
						(String)v1.getAs("method"), 
						(String)v1.getAs("endpoint"), 
						(String)v1.getAs("protocol"), 
						(String)v1.getAs("responseCode"), 
						(String)v1.getAs("contentSize"), 
						(String)v1.getAs("clientIdentd"));
        	};
        });
        
        map.foreach(new VoidFunction<ApacheAccessLog>() {
            /**
			 * 2018年9月5日下午3:14:22
			 * Tony
			 */
			private static final long serialVersionUID = 1L;

			@Override
            public void call(ApacheAccessLog s) throws Exception {
				System.out.println(s);
            }
        });
        
//        map.saveAsTextFile(args[1]);
        sc.stop();
        sc.close();
	}
}
