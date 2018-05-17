package indi.xiwu.hiveudf;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import java.io.File;
import java.net.InetAddress;
import org.apache.hadoop.io.Text;

/**
 * Created by xiwu on 18/5/15
 */
public class IPTransAddr extends UDF {
    public Text evaluate(Text ip) {
        if (ip == null) { return null; }
        File database = new File("/opt/database/GeoLite2-City.mmdb");
        try {
            DatabaseReader reader = new DatabaseReader.Builder(database).build();
            InetAddress ipAddress = InetAddress.getByName(ip.toString());
            CityResponse response = reader.city(ipAddress);
            String country = response.getCountry().getNames().get("zh-CN");
            String province = response.getMostSpecificSubdivision().getNames().get("zh-CN");
            String city = response.getCity().getNames().get("zh-CN");
            return new Text(country +","+ province +","+ city);
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public static void main(String[] args) {
        IPTransAddr ipTransAddr = new IPTransAddr();
        System.out.printf(ipTransAddr.evaluate(new Text("61.136.208.77")).toString());
    }
}
