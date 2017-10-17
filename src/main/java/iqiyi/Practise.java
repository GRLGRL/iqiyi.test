package iqiyi;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by gaoronglei_sx on 2017/9/11.
 */
public class Practise {
    public static void main(String[] args)
    {
        String str = "你好，世界!";
        byte[] buf = str.getBytes();
        char[] charArr = str.toCharArray();
        for(int i=0;i<buf.length;i++)
            System.out.print(buf[i]);
        System.out.println();
        for(int i=0;i<charArr.length;i++)
            System.out.print(charArr[i]+",");
        System.out.println();
       String v = Bytes.toString(buf);
       System.out.print(v);
    }
}
