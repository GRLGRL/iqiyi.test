package iqiyi;

/**
 * Created by gaoronglei_sx on 2017/8/8.
 */
import org.apache.aries.util.filesystem.FileSystem;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.Test;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReadDfsFileDemo {

    public static Logger logger = LoggerFactory.getLogger(ReadDfsFileDemo.class);

    public static String jobName;
    public static String queueName;
    public static String inputPath;
    public static String outputPath;
    public static String reduceNumber;

    private static Map<String, String> longyuanMap = new LinkedHashMap<String, String>();
    public static void main(String[] args)throws Exception
   {
       Configuration conf = new Configuration();
       if (args.length != 2) {
           System.out.println("Usage: MaxTemperature <input path> <out path>");
           System.exit(-1);
       }
       String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
       if (!parseArgs(otherArgs)) {
           System.exit(2);
       }
       Job job = new Job();
       job.setJarByClass(ReadDfsFileDemo.class);
       job.setJobName("Test File Scan.......");

       org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));
       org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));

       job.setMapperClass(FileMapper.class);
       job.setCombinerClass(FileReduce.class);
       job.setReducerClass(FileReduce.class);

       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);

       System.exit(job.waitForCompletion(true) ? 0 : 1);

   }
    private static boolean parseArgs(String[] args) throws Exception {
        Options opts = new Options();
        opts.addOption("jobName", "jobName", true, "Name of MR job");
        opts.addOption("queue", "queue", true, "MR queue to be used");
        opts.addOption("reduceNum", "reduceNum", true, "Reduce number");
        opts.addOption("inputPath", "inputPath", true, "Input path");
        opts.addOption("outputPath", "outputPath", true, "Output path");

        BasicParser parser = new BasicParser();
        CommandLine commandLine = parser.parse(opts, args);
        jobName = commandLine.getOptionValue("jobName");
        queueName = commandLine.getOptionValue("queue");
        reduceNumber = commandLine.getOptionValue("reduceNum");
        inputPath = commandLine.getOptionValue("inputPath");
        outputPath = commandLine.getOptionValue("outputPath");
        return true;
    }

   public static class FileMapper extends Mapper<Text,Text,Text,Text>
   {
       @Override
       protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
          String line = key.toString();
          String[] arr = line.split("\\t");
          if(arr.length >= 2)
          {
              context.write(new Text(arr[0]),new Text("1"));
          }
       }
   }

   public static class FileReduce extends Reducer<Text,Text,Text,Text>
   {
       @Override
       protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           String k = key.toString();
           int sum = 0;
           for(Text text : values)
           {
               sum ++;
           }
           context.write(key,new Text(String.valueOf(sum)));
       }
   }
}
