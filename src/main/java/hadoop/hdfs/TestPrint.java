package hadoop.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * 创建文件夹
 * ./hdfs dfs -mkdir -p /user/wenyuguang 
 * 存放文件
 * ./hdfs dfs -put /etc/passwd /user/wenyuguang/passwd.txts
 */
public class TestPrint {

	public static void main(String[] args) throws IOException {
		/*
		 * // 创建Configuration对象 Configuration conf=new Configuration(); //
		 * 创建FileSystem对象 FileSystem fs= FileSystem.get(URI.create(args[0]),conf); //
		 * FileSystem fs=
		 * FileSystem.get(URI.create("hdfs://10.1.89.50:9000/user/zyh/passwd"),conf); //
		 * 需求：查看/user/kevin/passwd的内容 // args[0] hdfs://10.1.89.50:9000/user/zyh/passwd
		 * // args[0] file:///etc/passwd FSDataInputStream is= fs.open(new
		 * Path(args[0])); byte[] buff=new byte[1024]; int length=0;
		 * while((length=is.read(buff))!=-1){ System.out.println(new
		 * String(buff,0,length)); } System.out.println(fs.getClass().getName());
		 */

		// 创建configuration对象
		Configuration conf = new Configuration();
		// 创建FileSystem对象
		// 需求：查看hdfs集群服务器/user/wenyuguang/passwd.txts的内容
		FileSystem fs = FileSystem.get(URI.create("hdfs://10.1.89.50:9000/user/wenyuguang/passwd.txts"), conf);
		// args[0] hdfs://1.0.0.3:9000/user/wenyuguang/passwd.txts
		// args[0] file:///etc/passwd.txt
		FSDataInputStream is = fs.open(new Path("hdfs://10.1.89.50:9000/user/wenyuguang/passwd.txts"));
		OutputStream os = new FileOutputStream(new File("a.txt"));
		byte[] buff = new byte[1024];
		int length = 0;
		while ((length = is.read(buff)) != -1) {
			System.out.println(new String(buff, 0, length));
			os.write(buff, 0, length);
			os.flush();
		}
		os.close();
		System.out.println(fs.getClass().getName());
		// 这个是根据你传的变量来决定这个对象的实现类是哪个
	}
}
