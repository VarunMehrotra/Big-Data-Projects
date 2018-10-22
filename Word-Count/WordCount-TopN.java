package WordProblem;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import java.util.*;

public class WordCount-TopN extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(WordCount-TopN.class);
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WordCount-TopN(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "WordCount-TopN");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map<S, I extends Number> extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable one = new DoubleWritable(1);
        private Text word = new Text();
        private boolean caseSensitive = false;

        protected void setup(Mapper.Context context)
                throws IOException,
                InterruptedException {
            Configuration config = context.getConfiguration();
            this.caseSensitive = config.getBoolean("WordCount-TopN.case.sensitive", false);
        }

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();
            List<String> list = new ArrayList<String>(Arrays.asList(line.split(",")));

            if(!list.get(1).equals("movieId") && !list.get(2).equals("rating")){
                context.write(new Text(list.get(1)), new DoubleWritable(Double.parseDouble(list.get(2))));
            }
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private java.util.Map<Text, DoubleWritable> map = new HashMap<Text, DoubleWritable>();

        @Override
        public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int flagCount = 0;

            for (DoubleWritable count : counts) {
                sum += count.get();
                flagCount++;
            }
            map.put(new Text(word), new DoubleWritable(sum/flagCount));
            context.write(new Text(word), new DoubleWritable(sum/flagCount));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            java.util.Map<Text, DoubleWritable> sortedMap = MiscUtils.sortByValues(map);
            String fileName = "Part2_Top20.txt";

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


