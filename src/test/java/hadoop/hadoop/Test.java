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
        String line = "150.37.147.4 - - [04/Aug/2018:02:56:40 +0800] \"GET /api/service-pingshan/getAjxx?ajbs=310300000005376&ajlx=33&accessToken=d0c5787b04ae11e8bc93000ec6cba075 HTTP/1.1\" 404 139 \"-\" \"Jakarta Commons-HttpClient/3.1\" \"-\"";
        String ip = line.split(" - - ")[0];
        String other = line.split(" - - ")[1];
        String time = other.split("0800]")[0].substring(1, other.split("0800]")[0].indexOf(" "));
        String other1 = other.split("0800] \"")[1];
        String method = other1.substring(0, other1.indexOf(" "));
        String url = other1.substring(other1.indexOf(" "), other1.indexOf("HTTP/1.1"));
        String status = line.split("HTTP/1.1\" ")[1].substring(0,3);
        String clientType = line.split("\"-\" \"")[1].replace("\" \"-\"", "");
        System.out.println(line.substring(0, line.indexOf(" - - ")));
    }
}
