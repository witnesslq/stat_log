package com.aicai.mrscript.mr.script;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * 
 * @author Young
 *
 */
public class StatAuthenticateUser {

	public static class StatAuthenticateUserMapper1 extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
		private Text uid = new Text();

		public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StatKey Stat = StatKey.filterUV(value.toString());
			// 判断请求是否异常且是授信接口
			if (Stat.isValid() && Stat.isAuthenticateUrl()) {
				word.set(Stat.getCookieChannel() + ":" + Stat.getUid());
				output.collect(word, uid);
				// b12 {1,2,3,4,5,6,7,8,1}
			}
		}
	}

	public static class StatAuthenticateUserReducer1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			output.collect(key, result);
		}
	}

	public static class StatAuthenticateUserMapper extends MapReduceBase
			implements Mapper<Object, Text, Text, IntWritable> {
		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String val = value.toString();
			String[] arr = val.split(":");
			if (arr[0] != "" && arr[0] != null) {
				word.set(arr[0]);
				output.collect(word, one);
			}
			// b12 {1,2,3,4,5,6,7,8,1}
		}
	}

	public static class StatAuthenticateUserReducer extends MapReduceBase
			implements Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			result.set(sum);
			output.collect(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://localhost:9000/user/hdfs/log_stat3";
		String output = "hdfs://localhost:9000/user/hdfs/log_Stat3/uv";
		String output2 = "hdfs://localhost:9000/user/hdfs/log_Stat3/uv3";
		JobConf conf = new JobConf(StatAuthenticateUser.class);

		// 第一个job的配置
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(StatAuthenticateUserMapper1.class);
		conf.setCombinerClass(StatAuthenticateUserReducer1.class);
		conf.setReducerClass(StatAuthenticateUserReducer1.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		JobClient.runJob(conf);
		// ======================
		JobConf conf2 = new JobConf(StatAuthenticateUser.class);
		conf2.setJobName("StatAuthenticateUser2");
		// 第二个作业的配置
		conf2.setMapOutputKeyClass(Text.class);
		conf2.setMapOutputValueClass(IntWritable.class);
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(IntWritable.class);
		conf2.setMapperClass(StatAuthenticateUserMapper.class);
		conf2.setCombinerClass(StatAuthenticateUserReducer.class);
		conf2.setReducerClass(StatAuthenticateUserReducer.class);

		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf2, new Path(output));
		FileOutputFormat.setOutputPath(conf2, new Path(output2));

		JobClient.runJob(conf2);
		System.exit(0);
	}

}
