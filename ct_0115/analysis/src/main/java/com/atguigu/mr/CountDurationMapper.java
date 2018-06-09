package com.atguigu.mr;

import com.atguigu.kv.key.CommDimension;
import com.atguigu.kv.key.ContactDimension;
import com.atguigu.kv.key.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountDurationMapper extends TableMapper<CommDimension, Text> {

    private Map<String, String> phoneName = new HashMap<String,String>();
    private Text v = new Text();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        init();
    }

    private void init() {
        phoneName.put("19251212343", "李雁");
        phoneName.put("15961260091", "卫艺");
        phoneName.put("17130206814", "仰莉");
        phoneName.put("18682499648", "陶欣悦");
        phoneName.put("15361960968", "施梅梅");
        phoneName.put("18356645821", "金虹霖");
        phoneName.put("17818674361", "魏明艳");
        phoneName.put("14266298447", "华贞");
        phoneName.put("13141904126", "华啟倩");
        phoneName.put("13157770954", "仲采绿");
        phoneName.put("19460860743", "卫丹");
        phoneName.put("14016550401", "戚丽红");
        phoneName.put("14314302040", "何翠柔");
        phoneName.put("17457157786", "钱溶艳");
        phoneName.put("15108090007", "钱琳");
        phoneName.put("15882276699", "缪静欣");
        phoneName.put("19694998088", "焦秋菊");
        phoneName.put("18264427294", "吕访琴");
        phoneName.put("17245432318", "沈丹");
        phoneName.put("16705495586", "褚美丽");
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //0x_13651234567_2019-02-21 13:13:13_13891234567_1_0180
        String rowkey = Bytes.toString(value.getRow());

        String[] split = rowkey.split("_");

        String flag = split[4];
        if ("0".equals(flag)) {
            return;
        }
        String call1 = split[1];
        String call2 = split[3];

        //2019-02-21 13:13:13
        String buildTime = split[2];
        String year = buildTime.substring(0, 4);
        String month = buildTime.substring(5, 7);
        String day = buildTime.substring(8, 10);

        String duration = split[5];

        //设置value的值
        v.set(duration);

        CommDimension commDimension = new CommDimension();

        //第一个联系人维度封装
        ContactDimension contactDimension = new ContactDimension();
        contactDimension.setName(phoneName.get(call1));
        contactDimension.setPhoneNum(call1);

        //年维度
        DateDimension yearDimension = new DateDimension(year, "-1", "-1");
        commDimension.setContactDimension(contactDimension);
        commDimension.setDateDimension(yearDimension);
        context.write(commDimension,v);

        //月维度
        DateDimension monthDimension = new DateDimension(year, month, "-1");
        commDimension.setDateDimension(monthDimension);
        context.write(commDimension,v);

        //天维度
        DateDimension dayDimension = new DateDimension(year, month, day);
        commDimension.setDateDimension(dayDimension);
        context.write(commDimension,v);

        //第二个联系人维度封装
        contactDimension.setName(phoneName.get(call2));
        contactDimension.setPhoneNum(call2);

        //年维度
        commDimension.setContactDimension(contactDimension);
        commDimension.setDateDimension(yearDimension);
        context.write(commDimension,v);

        //月维度
        commDimension.setDateDimension(monthDimension);
        context.write(commDimension,v);

        //天维度
        commDimension.setDateDimension(dayDimension);
        context.write(commDimension,v);
    }
}
