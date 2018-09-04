package hadoop.hdfs;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class P00032_HdfsDemo_PutFile_0010 extends Configured implements Tool {
	@Override
	public int run(String[] strings) throws Exception {
		Configuration configuration = getConf();
		String input = configuration.get("input");
		String output = configuration.get("output");
		LocalFileSystem inFs = FileSystem.getLocal(configuration);
		FileSystem outFs = FileSystem.get(URI.create(output), configuration);
		FSDataInputStream is = inFs.open(new Path(input));
		FSDataOutputStream os = outFs.create(new Path(output));
		IOUtils.copyBytes(is, os, 1024, true);
		System.out.println(os.getClass().getName());
		inFs.close();
		outFs.close();
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new P00032_HdfsDemo_PutFile_0010(), args));
	}
}