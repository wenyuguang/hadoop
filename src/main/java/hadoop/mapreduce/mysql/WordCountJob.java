package hadoop.mapreduce.mysql;

/**
 * @author Wen.Yuguang
 * @version V1.0
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: TODO
 * @date ${date} ${time}
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * MapReduce计算后直接将结果写入MySQL
 */
public class WordCountJob {
    public static String driverClass = "com.mysql.jdbc.Driver";
    public static String dbUrl = "jdbc:mysql://10.1.89.126:3306/test";
    public static String userName = "root";
    public static String passwd = "wenyuguang";
    public static String inputFilePath = "hdfs://192.168.211.3:9000/input/word.txt";
    public static String tableName = "keyWord";
    public static String [] fields = {"word","total"};

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        DBConfiguration.configureDB(conf,driverClass,dbUrl,userName,passwd);
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(WordCountJob.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setMapOutputKeyClass(Text.class);

            job.setMapperClass(WordsCountMapper.class);
            job.setReducerClass(WordsCountReducer.class);

            job.setJobName("MyWordCountDB");
            job.addArchiveToClassPath(new Path("/mysql-connector-java-5.1.38.jar"));
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            DBOutputFormat.setOutput(job,tableName,fields);

            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}