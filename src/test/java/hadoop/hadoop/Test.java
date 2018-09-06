package hadoop.hadoop;

/**
 * @author Wen.Yuguang
 * @version V1.0
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: TODO
 * @date ${date} ${time}
 */
public class Test {

    public static void main(String[] args) {
        String line = "192.1.129.6 - - [09/Aug/2018:02:30:02 +0800] \"GET /api/service-dy/getFile/c3R3al8zMDAxMDAwMDEwMjk0NTVfMjAwMDU1 HTTP/1.1\" 200 567979 \"-\" \"Java/1.7.0_45\" \"-\"";
        String ip = line.split(" - - ")[0];
        String other = line.split(" - - ")[1];
        String time = other.split("0800]")[0].substring(1, other.split("0800]")[0].indexOf(" "));
        String other1 = other.split("0800] \"")[1];
        String method = other1.substring(0, other1.indexOf(" "));
        String url = other1.substring(other1.indexOf(" "), other1.indexOf("HTTP/1.1"));
        String status = line.split("HTTP/1.1\" ")[1].substring(0,3);
        String clientType = line.split("\"-\" \"")[1].replace("\" \"-\"", "");
        System.out.println(line.substring(0, line.indexOf(" - - ")));
        String lines = "192.1.129.6 - - [09/Aug/2018:02:30:02 +0800] \"GET /api/service-dy/getFile/c3R3al8zMDAxMDAwMDEwMjk0NTVfMjAwMDU1 HTTP/1.1\" 200 567979 \"-\" \"Java/1.7.0_45\" \"-\"";
        String[] spilt = lines.split(" ");
        System.out.println(spilt);
        System.out.println(spilt.length);
    }
}
