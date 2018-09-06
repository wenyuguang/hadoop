package hadoop.hadoop;

/**
 * @author Wen.Yuguang
 * @version V1.0
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: TODO
 * @date ${date} ${time}
 */
public class NginxTest {

    public static void main(String[] args) {
        String lines = "192.1.129.6 - - [09/Aug/2018:02:30:02 +0800] \"GET /api/service-dy/getFile/c3R3al8zMDAxMDAwMDEwMjk0NTVfMjAwMDU1 HTTP/1.1\" 200 567979 \"-\" \"Java/1.7.0_45\" \"-\"";
        String[] log = lines.split(" ");
        System.out.println(log[0]);
        System.out.println(log[3].replace("[",""));
        System.out.println(log[5].replace("\"",""));
        System.out.println(log[6]);
        System.out.println(log[7].replace("\"",""));
        System.out.println(log[8]);
        System.out.println(log[9]);
        System.out.println(log[11].replace("\"",""));
    }
}
