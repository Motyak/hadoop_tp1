import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount
{
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
        { 
            // String string = value.toString();
            // if(!string.isEmpty())
            // {
            //     Text outText = new Text(string.substring(0,1));
            //     int val = ThreadLocalRandom.current().nextInt(1,100);
            //     IntWritable outVal = new IntWritable(val);
            //     context.write(outText, outVal);
            // }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        {
            // int sum = 0;
            // for(IntWritable value: values)
            // {
            //     sum = sum + value.get();
            // }
            // IntWritable outVal = new IntWritable(sum);
            // context.write(key, outVal);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "WordCount program");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
