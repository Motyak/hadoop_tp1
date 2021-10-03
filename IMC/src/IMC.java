import java.io.IOException;
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

public class IMC
{
    public static class IMCMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            /* Je tokenise le texte en mots */
            String[] tokens = value.toString().split("\\P{L}+");

            /* Je traite les tokens et écris les paires résultats */
            for(String str : tokens)
                if(!str.isBlank())
                    context.write(
                        new Text(str.toLowerCase()),
                        new IntWritable(1)
                    );
        }
    }

    public static class IMCReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            /* on calcule la somme des entiers */
            int sum = 0;
            for(IntWritable i : values)
                sum += i.get();

            /* On écrit le résultat dans une paire */
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "IMC program");
        job.setJarByClass(IMC.class);
        job.setMapperClass(IMCMapper.class);
        job.setReducerClass(IMCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
