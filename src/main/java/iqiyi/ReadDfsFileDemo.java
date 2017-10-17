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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReadDfsFileDemo {

    public static Logger logger = LoggerFactory.getLogger(ReadDfsFileDemo.class);
    private static String jobName;
    private static String queue;
    private static String inputTable;
    private static String outputTable;
    private static String reduceNum;
    private static String inputPath;
    private static String outputPath;

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
    public static void main(String[] args)throws Exception
   {
       JobConf conf = new JobConf(ReadDfsFileDemo.class);
       String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
       parseArgs(otherArgs);
       conf.set("mapreduce.job.queuename",queue);
       conf.setJobName(jobName);
       conf.setNumReduceTasks(Integer.parseInt(reduceNum));

       conf.setOutputKeyClass(Text.class);
       conf.setOutputValueClass(Text.class);

       conf.setMapperClass(FileMapper.class);
       conf.setCombinerClass(FileReduce.class);
       conf.setReducerClass(FileReduce.class);

       FileInputFormat.setInputPaths(conf, new Path(inputPath));
       FileOutputFormat.setOutputPath(conf, new Path(outputPath));

       JobClient.runJob(conf);
   }


   public static class FileMapper extends  MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
   {
       public void map(LongWritable key,Text value,OutputCollector<Text, Text> output,Reporter reporter) throws IOException
       {
           String line = value.toString().trim();  //2017-05-011204340784
           if(line.length() < 11)
               return;
           String k = line.substring(0,10);
           String v = line.substring(10);
           output.collect(new Text(k),new Text(v));
       }
   }

   public static class FileReduce  extends MapReduceBase implements Reducer<Text,Text, Text,Text>
   {
       public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output,Reporter reporter) throws IOException
       {
          int count = 0;
           while (values.hasNext())
           {
               count++;
           }
           output.collect(key,new Text(String.valueOf(count)));
       }
   }
}
