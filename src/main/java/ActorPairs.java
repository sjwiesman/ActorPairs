/**
 * @author  by sethwiesman on 11/25/14.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.Iterator;
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
                if (actors[i].trim().length() == 0) {
			continue;
		}

		for (int j = i+1; j < actors.length; ++j) {
                    if (actors[j].trim().length() == 0) {
			continue;
		    }
		    StringBuilder forwards = new StringBuilder(actors[i]);
                    forwards.append('\t').append(actors[j]);
                    context.write(new Text(forwards.toString()), NullWritable.get());

                    StringBuilder backwards = new StringBuilder(actors[j]);
                    backwards.append('\t').append(actors[i]);
                    context.write(new Text(backwards.toString()), NullWritable.get());
                }
            }
        }

    }


    public static class Reduce extends TableReducer<Text, NullWritable, Text> {

        public void reduce(Text key, Iterator<NullWritable> value, Context context) throws IOException, InterruptedException{
            Put put = new Put(key.getBytes());
            put.add(Bytes.toBytes("f1"), Bytes.toBytes("path"), key.getBytes());
            context.write(key, put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 1) {
            System.out.println("Error");
            return;
        }

        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf, "pairs");
        job.setJarByClass(ActorPairs.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob("paths", Reduce.class, job);
        job.setReducerClass(Reduce.class);
        job.waitForCompletion(true);
    }
}

