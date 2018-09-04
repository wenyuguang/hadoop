package hadoop.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * 通过设置命令行参数变量来编程
 * ./hadoop jar 包名 主类名 -Dinput=hdfs://10.1.89.50:9000/tmp -Doutput=/路径/文件
 */
public class GetDemo_0011 extends Configured implements Tool {
	@Override
	public int run(String[] strings) throws Exception {
		// 我们所有的代码都写在这个run方法中
		Configuration conf = getConf();
		String input = conf.get("input");
		String output = conf.get("output");
		FileSystem inFs = FileSystem.get(URI.create(input), conf);
		FSDataInputStream is = inFs.open(new Path(input));
		FileSystem outFs = FileSystem.getLocal(conf);
		FSDataOutputStream os = outFs.create(new Path(output));
		IOUtils.copyBytes(is, os, conf, true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// ToolRunner中的run方法中需要一个Tool的实现类，和
		System.exit(ToolRunner.run(new GetDemo_0011(), args));
	}
}