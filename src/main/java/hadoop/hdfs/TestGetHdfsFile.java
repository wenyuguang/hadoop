package hadoop.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * 获取HDFS中的文件
 *
 */
public class TestGetHdfsFile {

	public static void main(String[] args) throws IOException {
		
		System.setProperty("hadoop.home.dir", "D:\\hadoop-3.1.1\\hadoop-3.1.1");
		// 创建configuration对象
		Configuration conf = new Configuration();
		// 获取从集群上读取文件的文件系统对象
		// 和输入流对象
		FileSystem inFs = FileSystem.get(URI.create("hdfs://10.1.89.50:9000/tmp"), conf);
		FSDataInputStream is = inFs.open(new Path("hdfs://10.1.89.50:9000/tmp"));
		// 获取本地文件系统对象
		LocalFileSystem outFs = FileSystem.getLocal(conf);
		FSDataOutputStream os = outFs.create(new Path("jdk1.8.tar.gz"));
		byte[] buff = new byte[1024];
		int length = 0;
		while ((length = is.read(buff)) != -1) {
			os.write(buff, 0, length);
			os.flush();
		}
		System.out.println(inFs.getClass().getName());
		System.out.println(is.getClass().getName());
		System.out.println(outFs.getClass().getName());
		System.out.println(os.getClass().getName());
		os.close();
		is.close();
	}
}
