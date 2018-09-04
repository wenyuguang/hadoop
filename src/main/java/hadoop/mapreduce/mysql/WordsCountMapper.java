package hadoop.mapreduce.mysql;

/**
 * @author Wen.Yuguang
 * @version V1.0
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: TODO
 * @date ${date} ${time}
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map
 */
public class WordsCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取每一个输入行
        String line = value.toString();
        String ip = line.split(" - - ")[0];
        String other = line.split(" - - ")[1];
        String time = other.split("0800]")[0].substring(1, other.split("0800]")[0].indexOf(" "));
        String other1 = other.split("0800] \"")[1];
        String method = other1.substring(0, other1.indexOf(" "));
        String url = other1;

        context.write(new Text(ip), new IntWritable(1));
        context.write(new Text(time), new IntWritable(1));
        context.write(new Text(method), new IntWritable(1));
        context.write(new Text(url), new IntWritable(1));
    }
}