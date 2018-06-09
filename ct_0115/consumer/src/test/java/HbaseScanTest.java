import com.atguigu.utils.ConnectionInstance;
import com.atguigu.utils.HbaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.deser.StdDeserializer;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;

public class HbaseScanTest {

    //14314302040,2019-01,2019-04

    @Test
    public void scanData() throws ParseException, IOException {

        //14314302040 1月到4月所有童话数据
//        String phone = "14314302040";
//        String startPoint = "2019-01";
//        String endPoint = "2019-05";

        //05_14314302040_2019-01-19 06:29:03_15961260091_0954
        //05_14314302040_2019-01
        //05_14314302040_2019-02
        HbaseScanUtil scanUtil = new HbaseScanUtil();

        scanUtil.init("14314302040", "2018-10", "2019-03");

        Connection connection = ConnectionInstance.getInstance();
        Table table = connection.getTable(TableName.valueOf("ct:calllog"));

        while (scanUtil.hasNext()) {
            String[] rowkeys = scanUtil.next();
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(rowkeys[0]));
            scan.setStopRow(Bytes.toBytes(rowkeys[1]));

            //regionHash + "_" + phone + "_" + startTime;
            System.out.println("时间范围："
                    + rowkeys[0].split("_")[2]
                    + "----"
                    + rowkeys[1].split("_")[2]);
            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
            }
        }
    }
}
