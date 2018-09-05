package hadoop.spark.mysql;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author Wen.Yuguang
 * @version V1.0
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: TODO
 * @date ${date} ${time}
 */
public class SparkWriteMysql {
    public static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SparkWriteMysql.class);

    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkMysql").setMaster("spark://150.0.2.44:7077"));
        SQLContext sqlContext = new SQLContext(sparkContext);
        //写入的数据内容
        JavaRDD<String> personData = sparkContext.parallelize(Arrays.asList("1 tom 5","2 jack 6","3 alex 7"));
        //数据库内容
        String url = "jdbc:mysql://150.0.32.36:3306/test";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","wenyuguang");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");
        /**
         * 第一步：在RDD的基础上创建类型为Row的RDD
         */
        //将RDD变成以Row为类型的RDD。Row可以简单理解为Table的一行数据
        JavaRDD<Row> personsRDD = personData.map(new Function<String,Row>(){
            /**
			 * 2018年9月5日下午3:10:21
			 * Tony
			 */
			private static final long serialVersionUID = 1L;

			public Row call(String line) throws Exception {
                String[] splited = line.split(" ");
                return RowFactory.create(Integer.valueOf(splited[0]),splited[1],Integer.valueOf(splited[2]));
            }
        });

        /**
         * 第二步：动态构造DataFrame的元数据。
         */
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));

        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);

        /**
         * 第三步：基于已有的元数据以及RDD<Row>来构造DataFrame
         */
        Dataset<Row> personsDF = sqlContext.createDataFrame(personsRDD,structType);

        /**
         * 第四步：将数据写入到person表中
         */
        personsDF.write().mode("append").jdbc(url,"person",connectionProperties);

        //停止SparkContext
        sparkContext.stop();
    }
}