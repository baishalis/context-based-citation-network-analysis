package mcad;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class UniqueDOICitationContext {
	public static class InputMapper extends Mapper<LongWritable, Text, Text, Text> {
		
//		private HTable htablePaperbagOfWords;
//		private Configuration config;
//		
//		protected void setup(
//				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
//				throws IOException, InterruptedException {
//			config = HBaseConfiguration.create();
//			config.set("hbase.zookeeper.quorum", "172.17.25.18");
//			config.set("hbase.zookeeper.property.clientPort", "2183");
//			try {
//				htablePaperbagOfWords = new HTable(config, "PaperbagOfPhrases");
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] cols = value.toString().split("\\*\\*\\*");
			//Get toPaperRow = new Get(Bytes.toBytes(doit));
		//	Result toPaperRowResult = htablePaperbagOfWords.get(toPaperRow);
			//String str = Bytes.toString(toPaperRowResult.getValue(Bytes.toBytes("keyphrases"), Bytes.toBytes("keyphrases")));
//			if(str != null) {
			//	System.out.println(str);
			context.write(new Text(cols[1].trim()), new Text(cols[2].trim()));
			//}
			
		}
	}
	
	
	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		@Override
		protected void reduce(Text doit, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String paperdoi = doit.toString().trim();
			String mergedFilteredContext = "";
			for (Text val : values) {
				mergedFilteredContext += ";;" + val;
			}
			
			Put put = new Put(Bytes.toBytes(paperdoi));
			put.add(Bytes.toBytes("citationContext"), Bytes.toBytes("mergedContext"), Bytes.toBytes(mergedFilteredContext));

			context.write(new ImmutableBytesWritable(Bytes.toBytes(paperdoi)), put);

		}
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.18");
		conf.set("hbase.zookeeper.property.clientPort", "2183");
		
		Job job = new Job(conf, "UniqueDOICitationContext");
		job.setJarByClass(UniqueDOICitationContext.class);
		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));	// "filteredCitations.txt"

		TableMapReduceUtil.initTableReducerJob("DOIMergedContext", PaperEntryReducer.class, job);
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
	}

}