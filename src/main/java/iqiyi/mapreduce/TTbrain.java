package iqiyi.mapreduce;

/**
 * Created by gaoronglei_sx on 2017/9/4.
 */
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.io.IOException;



/**
 * Created by maohan_sx on 2017/7/4.
 */

public class TTbrain {
    public static Logger logger = LoggerFactory.getLogger(TTbrain.class);

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

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
        parseArgs(otherArgs);
        conf.set("mapreduce.job.queuename",queue);
        Job job = new Job(conf);
        job.setJarByClass(TTbrain.class);
        job.setNumReduceTasks(Integer.parseInt(reduceNum));
        job.setJobName(jobName);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(VideoDurationMapper.class);
        //job.setCombinerClass(VideoDurationReducer.class);
        job.setReducerClass(VideoDurationReducer.class);


        //job.setOutputFormatClass(TextOutputFormat.class);
        //job.setInputFormatClass(org.apache.hadoop.mapreduce.InputFormat.class);
        //设置输入文件路径
        FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));//设置输出文件路径
        boolean success=  job.waitForCompletion(true);

    }

    public static class VideoDurationMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] arr = line.split("\t");
            if(arr.length >= 2)
            context.write(new Text(arr[0]),new Text(arr[1]));
        }
    }

    public static class VideoDurationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
             int count = 0;
            for(Text text : values)
            {
                count++;
                //context.write(text,new Text("p"));
            }
            context.write(key,new Text(String.valueOf(count)));
        }
    }

}
