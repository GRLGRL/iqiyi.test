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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReadDfsFileDemo {

    public static Logger logger = LoggerFactory.getLogger(ReadDfsFileDemo.class);


    public static void main(String[] args)throws Exception
   {
       JobConf conf = new JobConf(ReadDfsFileDemo.class);
       conf.setJobName("WordCountRC");
       conf.setOutputKeyClass(Text.class);
       conf.setOutputValueClass(Text.class);
       conf.setMapperClass(FileMapper.class);
       conf.setCombinerClass(FileReduce.class);
       conf.setReducerClass(FileReduce.class);
       conf.setInputFormat(TextInputFormat.class);
       conf.setOutputFormat(TextOutputFormat.class);
       FileInputFormat.setInputPaths(conf, new Path(args[0]));
       FileOutputFormat.setOutputPath(conf, new Path(args[1]));
       JobClient.runJob(conf);
   }


   public static class FileMapper extends  MapReduceBase implements Mapper<Text, Text, Text, Text>
   {

       public void map(Text key,Text value,OutputCollector<Text, Text> output,Reporter reporter) throws IOException
       {
           String line = key.toString();
       String arr[] = line.trim().split("^A");

           if (arr.length >= 2) {
               output.collect(new Text(arr[0]), new Text(arr[1]));
           }
       }

   }

   public static class FileReduce  extends MapReduceBase implements Reducer<Text,Text, Text,Text>
   {

       public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,Reporter reporter) throws IOException
       {
           int i =0;
           while(values.hasNext())
           {
               i++;
               values.next();
           }
           output.collect(key,new Text(String.valueOf(i)));
       }
   }
}
