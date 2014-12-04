/**
 * @author  by sethwiesman on 11/25/14.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ActorPairs {

    public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {

        final Pattern pattern;

        public Map() {
            this.pattern =  Pattern.compile("(.+)\\s\\(\\d{4}\\)(.*)");
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            final String line = value.toString();
            final Matcher matcher = pattern.matcher(line);

            if (!matcher.find()) {
                return;
            }

            final String[] actors = matcher.group(2).split("/");

            if (actors.length < 2) {
                return;
            }

            for (int i = 0; i < actors.length -1 ; ++i) {
                
                final String a = actors[i].trim();

                if (a.length() == 0) {
			        continue;
		        }

		        for (int j = i+1; j < actors.length; ++j) {
                    
                    final String b = actors[j].trim();

                    if (b.length() == 0) {
			            continue;
		            }

                    if (a.equals(b)) {
                        continue;
                    }

                    StringBuilder forwards = new StringBuilder(a);
                    forwards.append('\t').append(b);
                    context.write(new Text(forwards.toString()), NullWritable.get());

                    StringBuilder backwards = new StringBuilder(b);
                    backwards.append('\t').append(a);
                    context.write(new Text(backwards.toString()), NullWritable.get());
                }
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 2) {
            System.out.println("Error");
            return;
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf, "actor pairs");
        job.setJarByClass(ActorPairs.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}

