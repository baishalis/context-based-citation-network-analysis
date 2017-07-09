package mcad;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CommunityCount {

	// =paperID:paperID, timestamp=1462111833838, value=0000C4BB
	// 0000C500AI column=class:class
	//

	public static class InputMapper extends TableMapper<Text, Text> {

		@Override
		protected void map(ImmutableBytesWritable row, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// process data for the row from the Result instance.
			if(Bytes.toString(value.getValue(Bytes.toBytes("community"), Bytes.toBytes("community"))) != null) {
				String classes = Bytes.toString(value.getValue(Bytes.toBytes("community"), Bytes.toBytes("community")));
				if(!classes.equals("NIL")) {
					String communityArr[] = classes.trim().split(",");
					//System.out.println(communityArr);
					for (String someclass: communityArr) {
						context.write(new Text(someclass), new Text("one"));
					}
				}
			}
			
		}
	}

	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		@Override
		protected void reduce(Text fromComm, Iterable<Text> counts, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (Text count : counts) {
				++sum;
			}
			// System.out.println(sum);

			Put put = new Put(Bytes.toBytes(fromComm.toString()));
			put.add(Bytes.toBytes("count"), Bytes.toBytes("count"), Bytes.toBytes(Integer.toString(sum)));
			context.write(new ImmutableBytesWritable(Bytes.toBytes(fromComm.toString())), put);

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.18");
		conf.set("hbase.zookeeper.property.clientPort", "2183");

		Job job = new Job(conf, "CountCommunityGraph");

		job.setJarByClass(CommunityCount.class);
		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Scan scan = new Scan();

		TableMapReduceUtil.initTableMapperJob("PaperbagOfPhrases", scan, InputMapper.class, Text.class, Text.class, job);

		TableMapReduceUtil.initTableReducerJob("CommunityMetricAnalysis", PaperEntryReducer.class, job);
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
	}
}
