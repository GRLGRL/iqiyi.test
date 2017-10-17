package Test;

import com.qiyi.hbase.HbaseConf;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import javax.xml.soap.Text;
import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * Created by gaoronglei_sx on 2017/10/17.
 */
public class InsertToHbase {
    public static void main(String[] args)
    {
        Configuration conf = new Configuration();
        HBaseConfiguration.create();
    }

    public static void addDataToHase(String rowKey,String family, LinkedHashMap<String,String> data)
    {
        Put put = new Put(Bytes.toBytes(rowKey));
        for(String key : data.keySet())
        {
            String v = data.get(key);
            put.add(Bytes.toBytes(family),Bytes.toBytes(key),Bytes.toBytes(v));
        }
    }

    public static class mapper extends Mapper<Long,Text,Text,Text>
    {
        @Override
        protected void map(Long key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] data = line.split("\t");
            if(data.length != 8)
                return;
            LinkedHashMap<String,String> dataItem = new LinkedHashMap<String, String>();
            for(int i=0;i<8;i++)
            {
                dataItem.put("score"+i,data[i]);
            }
            addDataToHase(String.valueOf(key),"score",dataItem);
        }
    }
}
