package iqiyi.mapreduce;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by gaoronglei_sx on 2017/9/5.
 */

public class CreateInoOfHeadLine {

    public static Logger logger = LoggerFactory.getLogger(CreateInoOfHeadLine.class);

    private static String jobName;
    private static String queue;
    private static String reduceNum;
    private static String inputTable;
    private static String outputTable;
    private static String inputPath;
    private static String outputPath;


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        parseArgs(otherArgs);
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("mapreduce.job.queuename", queue);

        Job job = new Job(hbaseConf, jobName);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setJarByClass(CreateInoOfHeadLine.class);
        job.setNumReduceTasks(Integer.valueOf(reduceNum));

        DistributedCache.addCacheFile(new URI(inputPath), job.getConfiguration());

        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(inputTable, scan, QueryInvalidMeinvTaskMapper.class, Text.class, Text.class, job);

        job.setReducerClass(QueryInvalidMeinvTaskReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static boolean parseArgs(String[] args) throws Exception {
        Options opts = new Options();
        opts.addOption("jobName", "jobName", true, "Name of MR job");
        opts.addOption("queue", "queue", true, "MR queue to be used");
        opts.addOption("reduceNum", "reduceNum", true, "Reduce number");
        opts.addOption("inputTable", "inputTable", true, "Input table name");
        opts.addOption("outputTable", "outputTable", true, "Output table name");
        opts.addOption("inputPath", "inputPath", true, "Input path");
        opts.addOption("outputPath", "outputPath", true, "Output path");

        BasicParser parser = new BasicParser();
        CommandLine commandLine = parser.parse(opts, args);
        jobName = commandLine.getOptionValue("jobName");
        queue = commandLine.getOptionValue("queue");
        reduceNum = commandLine.getOptionValue("reduceNum");
        inputTable = commandLine.getOptionValue("inputTable");
        outputTable = commandLine.getOptionValue("outputTable");
        inputPath = commandLine.getOptionValue("inputPath");
        outputPath = commandLine.getOptionValue("outputPath");
        return true;
    }

    public static class QueryInvalidMeinvTaskMapper extends TableMapper<Text, Text> {

        private static final byte[] HBASE_FAMILY = Bytes.toBytes("data");
        private static final byte[] HBASE_QUALIFIER_ISVALID = Bytes.toBytes("is_valid");//数据是否有效
        private static final byte[] HBASE_QUALIFIER_STATUS = Bytes.toBytes("available_status"); //下线的原
        private static final byte[] HBASE_UPLOADERID = Bytes.toBytes("uploader_id");
        private static final byte[] HBASE_CREATE_TIME = Bytes.toBytes("create_time");  //创建时间
        private static final byte[] HBASE_UPLOAD_TYPE = Bytes.toBytes("upload_type");
        private static final byte[] HBASE_ORIGINAL_SITE_NAME = Bytes.toBytes("original_site_name");
        private static final byte[] HBASE_CHANNEL_ID = Bytes.toBytes("channel_id");
        private Map<String, String> dateMap = new LinkedHashMap<String, String>();
        private Map<String, String> channelMap = new LinkedHashMap<String, String>();
        private Map<Integer,String> mapReflect = new HashMap<Integer, String>();
        private List<HashMap<String,String>> mapList = new ArrayList<HashMap<String,String>>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] caches = DistributedCache.getCacheFiles(context.getConfiguration());
            FileSystem fs = FileSystem.get(caches[0], context.getConfiguration());
            InputStream inputStream = fs.open(new Path(caches[0]));
            ZipInputStream zin = null;
            try {
                zin = new ZipInputStream(inputStream);
                ZipEntry zipEntry = null;
                int i = 0;
                while ((zipEntry = zin.getNextEntry()) != null) {
                    if (!zipEntry.isDirectory()) {
                        HashMap<String, String> idsMap = new HashMap<String, String>();
                        Scanner in = new Scanner(zin);
                        String uidFilePath = zipEntry.getName();
                        String[] filePathSplit = uidFilePath.split("/");
                        String pathNameAndEnd = filePathSplit[filePathSplit.length - 1];
                        String pathName = pathNameAndEnd.substring(0, pathNameAndEnd.indexOf("."));
                        if (uidFilePath.contains("ChannelId")) {
                            while (in.hasNextLine()) {
                                String[] Arr = in.nextLine().trim().split("\t");
                                if (Arr.length >= 2)
                                    channelMap.put(Arr[0], Arr[1]);
                            }
                            mapList.add(idsMap);
                            mapReflect.put(i, "");
                        } else {
                            while (in.hasNextLine()) {
                                String[] Arr = in.nextLine().trim().split("\t");
                                if (Arr.length >= 2)
                                    idsMap.put(Arr[0], Arr[1]);
                            }
                            mapList.add(idsMap);
                            mapReflect.put(i, pathName);
                        }
                        i++;
                    }
                }
            } catch (IOException e) {
                logger.info("[DataQueryTask]"+e.getMessage());
            } finally {
                if (zin != null) {
                    try {
                        zin.close();
                    } catch (IOException e) {
                        logger.info("[DataQueryTask] ZipInputStream close error");
                    }
                }
            }

//            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//            Calendar calendar = Calendar.getInstance();
//            calendar.setTime(new Date());
//            for (int i = 1; i <= 7; i++) {
//                calendar.add(Calendar.DAY_OF_MONTH, -1);
//                dateMap.put(sdf.format(calendar.getTime()), sdf.format(calendar.getTime()));
//            }
            dateMap.put("2017-08-01","2017-08-01");
            dateMap.put("2017-08-02","2017-08-02");
            dateMap.put("2017-08-03","2017-08-03");
            dateMap.put("2017-08-04","2017-08-04");
            dateMap.put("2017-08-05","2017-08-05");
            dateMap.put("2017-08-06","2017-08-06");
            dateMap.put("2017-08-07","2017-08-07");
            dateMap.put("2017-08-08","2017-08-08");
            dateMap.put("2017-08-09","2017-08-09");
            dateMap.put("2017-08-10","2017-08-10");
            dateMap.put("2017-08-11","2017-08-11");
            dateMap.put("2017-08-12","2017-08-12");
            dateMap.put("2017-08-13","2017-08-13");
            dateMap.put("2017-08-14","2017-08-14");
            dateMap.put("2017-08-15","2017-08-15");
            dateMap.put("2017-08-16","2017-08-16");
            dateMap.put("2017-08-17","2017-08-17");
            dateMap.put("2017-08-18","2017-08-18");
            dateMap.put("2017-08-19","2017-08-19");
            dateMap.put("2017-08-20","2017-08-20");
            dateMap.put("2017-08-21","2017-08-21");
            dateMap.put("2017-08-22","2017-08-22");
            dateMap.put("2017-08-23","2017-08-23");
            dateMap.put("2017-08-24","2017-08-24");
            dateMap.put("2017-08-25","2017-08-25");
            dateMap.put("2017-08-26","2017-08-26");
            dateMap.put("2017-08-27","2017-08-27");
            dateMap.put("2017-08-28","2017-08-28");
            dateMap.put("2017-08-29","2017-08-29");
            dateMap.put("2017-08-30","2017-08-30");
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

            byte[] isValidValue = value.getValue(HBASE_FAMILY, HBASE_QUALIFIER_ISVALID);//可以为null，但是值为FALSE时一定无效
            byte[] statusValue = value.getValue(HBASE_FAMILY, HBASE_QUALIFIER_STATUS);//有值就代表该行数据无效
            byte[] uploadtypeValue = value.getValue(HBASE_FAMILY, HBASE_UPLOAD_TYPE);//有值就代表该行数据无效
            byte[] createTimeValue = value.getValue(HBASE_FAMILY, HBASE_CREATE_TIME);//有值就代表该行数据无效
            byte[] origisitenameValue = value.getValue(HBASE_FAMILY, HBASE_ORIGINAL_SITE_NAME);
            byte[] uidValue = value.getValue(HBASE_FAMILY, HBASE_UPLOADERID);
            byte[] channelValue = value.getValue(HBASE_FAMILY, HBASE_CHANNEL_ID);
            if (isValidValue != null && Boolean.parseBoolean(Bytes.toString(isValidValue)) == Boolean.FALSE) {
                return;
            }
            if (statusValue != null) {
                return;
            }
            if (uidValue == null)
                return;

            //判断是否是抓取
            if (uploadtypeValue == null)
                return;
            if (!("1".equals(Bytes.toString(uploadtypeValue))))
                return;
            if (origisitenameValue == null)
                return;
            //判断创建时间
            if (createTimeValue == null)
                return;
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            long createTimeL =  Long.parseLong(Bytes.toString(createTimeValue));
            String createTimeStr = df.format(createTimeL);
            if (!dateMap.containsKey(createTimeStr))
                return;
            //频道
            String channels = "";
            if(channelValue != null)
            {
                StringBuilder channelSb = new StringBuilder("");
                String[] channelArr = Bytes.toString(channelValue).trim().split(",");
                for(int i = 0;i< channelArr.length;i++)
                {
                    String channelName = channelMap.get(channelArr[i].trim());
                    if(channelName == null || channelName.equals(""))
                        channelSb.append(channelArr[i]+"#");
                    else
                        channelSb.append(channelName+"#");
                }
                if(channelSb.length() > 0)
                    channels = channelSb.substring(0,channelSb.length()-1).toString();
            }
            String uid = Bytes.toString(uidValue);
            String origiSiteName = Bytes.toString(origisitenameValue);
            for(int i=0;i<mapList.size();i++)
            {
                Map<String,String> idsMap = mapList.get(i);
                if (idsMap.size() == 0)
                    continue;
                else
                {
                    if(idsMap.containsKey(uid))
                    {
                        String siteName = mapReflect.get(i);
                        if(siteName.contains("weixin"))
                        {
                            if(origiSiteName.contains("weixingzh") || origiSiteName.contains("toutiao"))
                            {
                                context.write(new Text(siteName+"\t" + idsMap.get(uid) + "\t" + uid), new Text(channels));
                            }
                        }
                        else if(siteName.contains("weibo"))
                        {
                            if(origiSiteName.contains("weibogzh") || origiSiteName.contains("toutiao"))
                            {
                                context.write(new Text(siteName+"\t" + idsMap.get(uid) + "\t" + uid), new Text(channels));
                            }
                        }
                        else
                        {
                            if(origiSiteName.indexOf(siteName) > -1)
                            {
                                context.write(new Text(siteName+"\t" + idsMap.get(uid) + "\t" + uid), new Text(channels));
                            }
                        }

                    }
                }
            }
        }
    }
    public static class QueryInvalidMeinvTaskReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String cateName = key.toString();
            double sum = 0;
            Set<String> channelSet = new HashSet<String>();
            for(Text text:values)
            {
                sum++;
                String[] channelList = text.toString().split("#");
                for(String channelItem: channelList)
                    channelSet.add(channelItem);
            }
            StringBuilder channelSb = new StringBuilder("");
            String channelValues = "";
            for(String channelItem : channelSet)
                if(!channelItem.equals(""))
                {
                    channelSb.append(channelItem+"、");
                }
            if(channelSb.length() > 0)
            {
                channelValues = channelSb.substring(0,channelSb.length()-1).toString();
            }
            String avg = String.format("%.2f",sum/7.0);
            context.write(key, new Text(avg+"\t"+channelValues));
        }
    }
}
