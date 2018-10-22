package WordProblem;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import java.util.*;
import java.io.*;

public class WordCount extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(WordCount.class);
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new part1(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "part1");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map<S, I extends Number> extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private boolean caseSensitive = false;
        private static final Pattern WORD_BOUNDARY = Pattern.compile("[^a-zA-Z0-9]");

        List<String> list = StopWord.stopWordList();

        protected void setup(Mapper.Context context)
                throws IOException,
                InterruptedException {
            Configuration config = context.getConfiguration();
            this.caseSensitive = config.getBoolean("part1.case.sensitive", false);
        }

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();
            if (!caseSensitive) {
                line = line.toLowerCase();
            }
            Text currentWord = new Text();
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty() || word.length() < 5 || list.contains(word)) {
                    continue;
                }
                currentWord = new Text(word.trim());
                context.write(currentWord,one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private java.util.Map<Text, IntWritable> map = new HashMap<Text, IntWritable>();

        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            map.put(new Text(word), new IntWritable(sum));
            context.write(new Text(word), new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            java.util.Map<Text, IntWritable> sortedMap = MiscUtils.sortByValues(map);
            String fileName = "Part1_Top20.txt";

            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true));

            int count = 0;
            for(Text key : sortedMap.keySet()) {
                if(count == 20){
                    writer.flush();
                    break;
                }
                String str = key.toString() + " " + sortedMap.get(key).toString() + "\n";
                writer.append(str);
                count++;
            }
            String jobName = context.getJobName();
            Configuration conf = context.getConfiguration();

            Job job = Job.getInstance(conf, jobName);
            String dest = FileOutputFormat.getOutputPath(job).toString();
            FileCopy.copyToHDFS(fileName, dest + "/" + fileName);

        }
    }
}


