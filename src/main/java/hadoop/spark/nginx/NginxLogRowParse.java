package hadoop.spark.nginx;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class NginxLogRowParse {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("nginx日志解析").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        
        // 创建lines RDD  "hdfs://150.0.2.44:9000/log/nginxlog/log.log"
        JavaRDD<String> lines = sc.textFile(args[0]);
        /**
         * 第一步：在RDD的基础上创建类型为Row的RDD
         */
        //首先，必须将RDD变成以Row为类型的RDD。Row可以简单理解为Table的一行数据
        JavaRDD<Row> personsRDD = lines.map(new Function<String,Row>(){

            /**
			 * 
			 */
			private static final long serialVersionUID = 2259801488731081294L;

			@Override
            public Row call(String line) throws Exception {
                String[] log = line.split(" ");
                if(log.length == 13||log.length == 12) {
                	return RowFactory.create(log[0],log[3].replace("[",""),log[5].replace("\"",""),
                			log[6],log[7],log[8],log[9],log[11].replace("\"",""));
                }
                return RowFactory.create("","","","","","","","");
            }
        });

        /**
         * 第二步：动态构造DataFrame的元数据，一般而言，有多少列以及每列的具体类型可能来自于
         * JSON文件，也可能来自于DB
         */
        //对Row具体指定元数据信息。
        List<StructField> structFields = new ArrayList<StructField>();
        //列名称  列的具体类型（Integer Or String） 是否为空一般为true，实际在开发环境是通过for循环，而不是手动添加
        structFields.add(DataTypes.createStructField("ipAddress",    DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("clientIdentd", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("dateTime",     DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("method",       DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("endpoint",     DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("protocol",     DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("responseCode", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("contentSize",  DataTypes.StringType, true));
        //构建StructType,用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);

        /**
         * 第三步：基于已有的MetaData以及RDD<Row>来构造DataFrame
         */
        Dataset<?> personsDF = sqlContext.createDataFrame(personsRDD, structType);

        /**
         * 第四步：注册成临时表以供后续的SQL查询操作
         */
        personsDF.registerTempTable("log");

        /**
         * 第五步：进行数据的多维度分析
         */
        Dataset<Row> result = sqlContext.sql("select * from log");
        /**
         * 第六步：对结果进行处理，包括由DataFrame转换成为RDD<Row>,以及结果的持久化
         */
        List<Row> listRow = result.javaRDD().collect();
        for(Row row : listRow){
            System.out.println(row);
        }

        
//        map.saveAsTextFile(args[1]);
        sc.stop();
        sc.close();
	}
}
