package hadoop.spark.mysql;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class SparkReadMysql {
	public static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SparkReadMysql.class);

	public static void main(String[] args) {
		JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf().setAppName("SparkReadMysql").setMaster("spark://150.0.2.44:7077"));
		SQLContext sqlContext = new SQLContext(sparkContext);
		// 读取mysql数据
		readMySQL(sqlContext, args[0]);

		// 停止SparkContext
		sparkContext.stop();
	}

	private static void readMySQL(SQLContext sqlContext, String table) {
		// jdbc.url=jdbc:mysql://localhost:3306/database
		String url = "jdbc:mysql://150.0.32.36:3306/test";
		// 查找的表名
//		String table = "user_test";
		// 增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "wenyuguang");
		connectionProperties.put("driver", "com.mysql.jdbc.Driver");

		// SparkJdbc读取Postgresql的products表内容
		System.out.println("读取test数据库中的user_test表内容");
		// 读取表中所有数据
		Dataset<Row> jdbcDF = sqlContext.read().jdbc(url, table, connectionProperties).select("*");
		// 显示数据
		jdbcDF.show();
	}
}