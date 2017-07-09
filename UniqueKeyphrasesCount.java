package mcad;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class UniqueKeyphrasesCount {
	
	public static int tableRowId = 1;
	
	public static class InputMapper extends Mapper< LongWritable, Text, Text, Text> {
	//	private final IntWritable ONE = new IntWritable(1);
	   	private Text text = new Text();

	   	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	   			String keyphrases = value.toString().trim();
	   			context.write(new Text(keyphrases), new Text(Integer.toString(tableRowId)));
//	   			++tableRowId;
	   		}
	
		}
	
	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		public void reduce(Text keyword, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		
			String phrases = keyword.toString().trim();
			String id = Integer.toString(tableRowId);
			Put put = new Put(Bytes.toBytes(id));
			put.add(Bytes.toBytes("keyphrase"), Bytes.toBytes("paperKeyphrases"), Bytes.toBytes(phrases.trim()));

			context.write(new ImmutableBytesWritable(Bytes.toBytes(id)), put);
			tableRowId++;
		}
		
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.18");
		conf.set("hbase.zookeeper.property.clientPort", "2183");
		
		Scan scan = new Scan();
		scan.setCaching(500);      
		scan.setCacheBlocks(false); 
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "UniqueKeyphrasesCount");
		job.setJarByClass(UniqueKeyphrasesCount.class);
		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));	// "part1.txt"
		
		
		//TableMapReduceUtil.initTableMapperJob("PaperbagOfPhrases", scan, InputMapper.class, Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("GlobalKeyphraseList", PaperEntryReducer.class, job);
		
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
	}

}
