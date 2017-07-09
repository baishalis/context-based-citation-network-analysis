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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

public class CreateContextCommunityGraph {

	public static class InputMapper extends TableMapper<Text, Text> {

		private HTable htablePaperBagOfWords;
		private Configuration config;

		@Override
		protected void setup(
				org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "172.17.25.18");
			config.set("hbase.zookeeper.property.clientPort", "2183");
			try {
				htablePaperBagOfWords = new HTable(config, "PaperbagOfPhrases");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		protected void map(ImmutableBytesWritable row, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String fromPaper = Bytes.toString(value.getValue(Bytes.toBytes("fromPaper"), Bytes.toBytes("fromPaper")));
			String toPapersDirect = new String(value.getValue(Bytes.toBytes("toPapersDirect"), Bytes.toBytes("toPapersDirect")));
			String toPapersDirectList[] = toPapersDirect.split(",");
			Get fromPaperRow = new Get(Bytes.toBytes(fromPaper));
			Result fromPaperRowResult = htablePaperBagOfWords.get(fromPaperRow);
			if(value.getValue(Bytes.toBytes("toPapersIndirect"), Bytes.toBytes("toPapersIndirect")) != null) {
				String toPapers = new String(value.getValue(Bytes.toBytes("toPapersIndirect"), Bytes.toBytes("toPapersIndirect")));
				if(!toPapers.equals("NIL") || toPapers.isEmpty()) {
					String toPaperList[] = toPapers.trim().split(",");
					for (String toPaper: toPaperList){
						toPaper = toPaper.trim();
						Get toPaperRow = new Get(Bytes.toBytes(toPaper));
						Result toPaperRowResult = htablePaperBagOfWords.get(toPaperRow);
						
						if (!fromPaperRowResult.isEmpty() && !toPaperRowResult.isEmpty()) {	// if both the papers belong to computer science communities
							Text one = new Text("1");
							String fromComm = Bytes
									.toString(fromPaperRowResult.getValue(Bytes.toBytes("community"), Bytes.toBytes("community"))).trim();
							String toComm = Bytes
									.toString(toPaperRowResult.getValue(Bytes.toBytes("community"), Bytes.toBytes("community"))).trim();
							// Get communities for both the papers
							if(!fromComm.equals("NIL") && !toComm.equals("NIL")) {
								String fromClasses[] = Bytes
										.toString(fromPaperRowResult.getValue(Bytes.toBytes("community"), Bytes.toBytes("community"))).split(",");
								String toClasses[] = Bytes
										.toString(toPaperRowResult.getValue(Bytes.toBytes("community"), Bytes.toBytes("community"))).split(",");

								for (int i = 0; i < fromClasses.length; i++) {
									for (int j = 0; j < toClasses.length; j++) {
										context.write(new Text(fromClasses[i] + "," + toClasses[j]), one);	// concat with comma and send to reducer
									}
								}
							}
						}
					}
				}
				
			}
			
			for (String toPaper: toPapersDirectList){
				toPaper = toPaper.trim();
				Get toPaperRow = new Get(Bytes.toBytes(toPaper));
				Result toPaperRowResult = htablePaperBagOfWords.get(toPaperRow);
				
				if (!fromPaperRowResult.isEmpty() && !toPaperRowResult.isEmpty()) {	// if both the papers belong to computer science communities
					Text one = new Text("1");
					if(toPaperRowResult.getValue(Bytes.toBytes("community"), Bytes.toBytes("community")) != null && fromPaperRowResult.getValue(Bytes.toBytes("community"), Bytes.toBytes("community")) != null) {
						String fromComm = Bytes
								.toString(fromPaperRowResult.getValue(Bytes.toBytes("community"), Bytes.toBytes("community"))).trim();
						String toComm = Bytes
								.toString(toPaperRowResult.getValue(Bytes.toBytes("community"), Bytes.toBytes("community"))).trim();
						// Get communities for both the papers
						if(!fromComm.equals("NIL") && !toComm.equals("NIL")) {
							String fromClasses[] = Bytes
									.toString(fromPaperRowResult.getValue(Bytes.toBytes("community"), Bytes.toBytes("community"))).split(",");
							String toClasses[] = Bytes
									.toString(toPaperRowResult.getValue(Bytes.toBytes("community"), Bytes.toBytes("community"))).split(",");

							for (int i = 0; i < fromClasses.length; i++) { 
								for (int j = 0; j < toClasses.length; j++) {
									context.write(new Text(fromClasses[i] + "," + toClasses[j]), one);	// concat with comma and send to reducer
								}
							}
						}
					}
				}
			}
			
			System.out.println(fromPaper + "");

			
		}
	}

	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		@Override
		protected void setup(Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {

		}

		@Override
		protected void reduce(Text classPair, Iterable<Text> ValueOne, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (Text One : ValueOne) {	// each pair of form `C1,C2` counts as an edge from C1 to C2
				++count;
			}

			String fromcomm = classPair.toString().split(",")[0].trim();	// get from community
			String tocomm = classPair.toString().split(",")[1].trim();		// get to community

			Put put = new Put(Bytes.toBytes(classPair.toString()));
			put.add(Bytes.toBytes("from"), Bytes.toBytes("fromComm"), Bytes.toBytes(fromcomm));
			put.add(Bytes.toBytes("to"), Bytes.toBytes("toComm"), Bytes.toBytes(tocomm));
			put.add(Bytes.toBytes("edgeCount"), Bytes.toBytes("count"), Bytes.toBytes(count + ""));
			context.write(new ImmutableBytesWritable(Bytes.toBytes(classPair.toString())), put);	// write to table

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.18");
		conf.set("hbase.zookeeper.property.clientPort", "2183");

		Job job = new Job(conf, "CreateContextCommunityGraph");
		job.setJarByClass(CreateContextCommunityGraph.class);
		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("CitationNetworksFinal", scan, InputMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("CommunityNetworkForIndirect", PaperEntryReducer.class, job);
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
	}
}
