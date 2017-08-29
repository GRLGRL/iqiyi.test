package iqiyi;

/**
 * Created by gaoronglei_sx on 2017/8/8.
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
public class FileMerge {
    private static Map<String,Integer>  rs = new LinkedHashMap<String, Integer>();
    public void setTime() {
        String endtimeString = "2017-08-08";
        String starttimeString = "2017-03-31";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        try {
            Date d = sdf.parse(endtimeString);
            calendar.setTime(d);
            while (true) {
                calendar.add(Calendar.DAY_OF_MONTH, -1);
                String time = sdf.format(calendar.getTime());
                if (time.equals(starttimeString))
                    break;
                else {
                    //longyuanMap.put(time, "");
                }
            }
        } catch (Exception e) {
            e.toString();
        }
    }
    public static  void main(String[] args)
    {
       String origin = "C:\\Users\\Administrator\\Desktop\\login_puID.txt";
       String later = "C:\\Users\\Administrator\\Downloads\\DAVYANZHENG";
       String output = "C:\\Users\\Administrator\\Desktop\\AAA.txt";
        merge(origin,later,output);
        System.out.println("Success");
    }
    public static void merge(String inputPath1,String inputPath2,String outputPath)
    {
        BufferedReader bfr1 = null;
        BufferedReader bfr2 = null;
        BufferedWriter bfw = null;
        try{
            bfr1 = new BufferedReader(new FileReader(inputPath1));
            String line = null;
            while ((line = bfr1.readLine())!= null)
            {
                rs.put(line.trim(),0);
            }
            String line2 = null;
            bfr2 = new BufferedReader(new FileReader(inputPath2));
            while ((line2 = bfr2.readLine())!= null)
            {
                String key = line2.split("\t")[0]+"\t"+line2.split("\t")[1];
                if(rs.containsKey(key))
                {

                    int count = Integer.parseInt(line2.split("\t")[2]);

                     rs.put(key,count);
                }
            }
            bfw = new BufferedWriter(new FileWriter(outputPath));
            for(String key : rs.keySet())
            {
                StringBuilder sb = new StringBuilder("");
                sb.append(key).append("\t").append(rs.get(key));
                System.out.println(sb.toString());
                bfw.write(sb.toString());
                bfw.newLine();
            }
            bfw.flush();

        }catch (Exception e)
        {

        }
        finally {
            try{
                if(bfr1 != null)
                {
                    bfr1.close();
                }
                if(bfr2 != null)
                {
                    bfr2.close();
                }
                if(bfw != null)
                {
                    bfw.close();
                }
            }catch (Exception e)
            {
                e.printStackTrace();
            }

        }

    }
}
