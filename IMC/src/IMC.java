import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class IMC
{
    public static class IMCMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String strValue = value.toString();
            if(strValue.isEmpty() || strValue.charAt(0) == '"')
                return;

            /* on parse les infos de l'individu */
            String[] columns = strValue.split(",");
            int age = Integer.parseInt(columns[1]);
            if(age < 18)
                return;
            int height = Integer.parseInt(columns[2]);
            int weight = Integer.parseInt(columns[3]);
            String sex = columns[4];

            /* on calcule l'IMC */
            double imc = ((double)weight / (double)height / (double)height) * 10000;

            /* on écrit le résultat dans une paire */
            context.write(
                new Text(sex), 
                new DoubleWritable(imc)
            );
        }
    }

    public static class IMCReducer extends Reducer<Text, DoubleWritable, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
            /* on stocke chaque IMC dans un tableau */
            List<Double> imcs = new ArrayList<>();
            for(DoubleWritable n : values)
                imcs.add(n.get());

            /* on calcule le minimum, maximum
               et la moyenne */
            double min = 9999.0, max = 0.0, sum = 0.0; 
            int length = 0;
            for(double imc : imcs)
            {
                if(imc < min)
                    min = imc;
                if(imc > max)
                    max = imc;
                sum += imc;
                ++length;
            }
            double mean = sum / length;

            /* on calcule l'ecart type */
            double ecartType = 0.0;
            for(double imc : imcs)
                ecartType += Math.pow(imc - mean, 2);
            ecartType = Math.sqrt(ecartType / length);

            /* on ecrit les resultats dans une paire */
            context.write(
                new Text(key.toString() + ": "),
                new Text("moy=" + mean + ";ecart-type=" 
                        + ecartType + ";min=" + min + ";max=" + max)
            );
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
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
