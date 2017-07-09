package mcad;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
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

public class MetricConductanceIndirect {

	public static class InputMapper extends TableMapper<Text, Text> {

		@Override
		protected void map(ImmutableBytesWritable row, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// process data for the row from the Result instance.
			String fromClass = Bytes.toString(value.getValue(Bytes.toBytes("from"), Bytes.toBytes("fromComm")));
			String toClass = new String(value.getValue(Bytes.toBytes("to"), Bytes.toBytes("toComm")));
			// create 'CommunityClass', 'time', 'count', 'metrics'
			// int edgeCount = Integer.parseInt(new
			// String(value.getValue(Bytes.toBytes("edgeCount"),
			// Bytes.toBytes("count"))));
			String edgeCount = Bytes.toString(value.getValue(Bytes.toBytes("edgeCount"), Bytes.toBytes("count")));

			if (!fromClass.equals(toClass)) {
				context.write(new Text(fromClass), new Text(edgeCount));
			}

		}
	}

	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		Map<String, Integer> intraEdgeCount = new HashMap<>();
		int totalPapers = 0;

		@Override
		protected void setup(Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "172.17.25.18");
			config.set("hbase.zookeeper.property.clientPort", "2183");

			HTable htableCommunityAbout = new HTable(config, "CommunityNetworkForIndirect");
			
			Scan scan = new Scan();
			ResultScanner scanner = htableCommunityAbout.getScanner(scan);
			
			for (Result result = scanner.next(); (result != null); result = scanner.next()) {
				String fromClass = Bytes.toString(result.getValue(Bytes.toBytes("from"), Bytes.toBytes("fromComm")));
				String toClass = Bytes.toString(result.getValue(Bytes.toBytes("to"), Bytes.toBytes("toComm")));
				if (fromClass.equals(toClass)) {
					String edgeCount = Bytes.toString(result.getValue(Bytes.toBytes("edgeCount"), Bytes.toBytes("count")));
					intraEdgeCount.put(fromClass, Integer.parseInt(edgeCount));
				}
			}
			
			
		}

		@Override
		protected void reduce(Text fromClass, Iterable<Text> counts, Context context)
				throws IOException, InterruptedException {


			double sum = 0;
			for (Text count : counts) {
				sum += Integer.parseInt(count.toString());
			}
		//	if(intraEdgeCount.get(fromClass.toString()) != null) {
				double conductance = ((double)sum / (intraEdgeCount.get(fromClass.toString()) + sum));
				//System.out.println(fromClass + "\t" + conductance);

				Put put = new Put(Bytes.toBytes(fromClass.toString()));
				put.add(Bytes.toBytes("metricsIndirect"), Bytes.toBytes("conductance"), Bytes.toBytes(Double.toString(conductance)));
				context.write(new ImmutableBytesWritable(Bytes.toBytes(fromClass.toString())), put);
		//	}

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.18");
		conf.set("hbase.zookeeper.property.clientPort", "2183");

		Job job = new Job(conf, "Conductance");

		job.setJarByClass(MetricConductanceIndirect.class);
		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Scan scan = new Scan();
		// scan.setStartRow(Bytes.toBytes("0000004B0045E6AD"));
		// scan.setStopRow(Bytes.toBytes("00004C2F01666599"));
		// scan.setCaching(50); // 1 is the default in Scan
		// scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob("CommunityNetworkForIndirect", scan, InputMapper.class, Text.class, Text.class, job);

		TableMapReduceUtil.initTableReducerJob("CommunityMetricAnalysis", PaperEntryReducer.class, job);
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
		// if (job.isSuccessful()) {
		//// System.out.println("completionTime :" + (job.getFinishTime() -
		// job.getStartTime()) / 1000 + "s");
		// }
	}
}