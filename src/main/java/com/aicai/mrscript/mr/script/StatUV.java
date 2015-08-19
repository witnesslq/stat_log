package com.aicai.mrscript.mr.script;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class StatUV {
	/**
	 * mysql
	 * 
	 * @author Young
	 *
	 */
	public static class BaseRecord implements Writable, DBWritable {
		int id;
		String name;

		public BaseRecord() {

		}

		public void readFields(DataInput in) throws IOException {
			this.id = in.readInt();
			this.name = Text.readString(in);
		}

		public String toString() {
			return new String(this.id + " " + this.name);
		}

		public void write(PreparedStatement stmt) throws SQLException {
			stmt.setInt(1, this.id);
			stmt.setString(2, this.name);
		}

		public void readFields(ResultSet result) throws SQLException {
			this.id = result.getInt(1);
			this.name = result.getString(2);
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(this.id);
			Text.writeString(out, this.name);
		}
	}

	public static class StatUVMapper1 extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
		private Text uid = new Text();

		public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StatKey Stat = StatKey.filterUV(value.toString());

			if (Stat.isValid()) {
				word.set(Stat.getChannel() + ":" + Stat.getUid());
				output.collect(word, uid);
				// b12 {1,2,3,4,5,6,7,8,1}
			}
		}
	}

	public static class StatUVReducer1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			output.collect(key, result);
		}
	}

	public static class StatUVMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
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

	public static class StatUVReducer extends MapReduceBase implements Reducer<Text, IntWritable, BaseRecord, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<BaseRecord, Text> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			result.set(sum);
			BaseRecord record = new BaseRecord();
			record.id = Integer.parseInt(result.toString());
			record.name = key.toString();
			output.collect(record, key);
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		String input = "hdfs://localhost:9000/user/hdfs/log_stat";
		String output = "hdfs://localhost:9000/user/hdfs/log_stat/uv" + System.currentTimeMillis()+""+System.nanoTime();
		JobConf conf = new JobConf(StatUV.class);

		// 第一个job的配置
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(StatUVMapper1.class);
		conf.setCombinerClass(StatUVReducer1.class);
		conf.setReducerClass(StatUVReducer1.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		JobClient.runJob(conf);
		// ======================
		JobConf conf2 = new JobConf(StatUV.class);
		conf2.setJobName("StatUV2");
		DistributedCache.addFileToClassPath(new Path("/hdfsPath/mysql-connector-java-5.1.30.jar"), conf2);

		// 第一个去重复， 第二个作业统计信息
		conf2.setMapOutputKeyClass(Text.class);
		conf2.setMapOutputValueClass(IntWritable.class);
		conf2.setOutputKeyClass(BaseRecord.class);
		conf2.setOutputValueClass(Text.class);
		conf2.setMapperClass(StatUVMapper.class);
		// 不需要捆绑Combiner
		// conf2.setCombinerClass(StatUVReducer.class);
		conf2.setReducerClass(StatUVReducer.class);

		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(DBOutputFormat.class);
		DBConfiguration.configureDB(conf2, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.0.122:3306/test1", "root",
				"123456");
		FileInputFormat.setInputPaths(conf2, new Path(output));
		DBOutputFormat.setOutput(conf2, "t", "id", "name");
		JobClient.runJob(conf2);
		System.exit(0);
	}

}
