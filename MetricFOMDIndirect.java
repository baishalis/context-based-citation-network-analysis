package mcad;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MetricFOMDIndirect {

	public static class InputMapper extends TableMapper<Text, Text> {

	//	HTable htablePaperAbout;

		//@Override
//		protected void setup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
//				throws IOException, InterruptedException {
//			// TODO Auto-generated method stub
//			// super.setup(context);
//			Configuration config = HBaseConfiguration.create();
//			config.set("hbase.zookeeper.quorum", "172.17.25.20");
//			config.set("hbase.zookeeper.property.clientPort", "2183");
//
//			htablePaperAbout = new HTable(config, "PaperAbout");
//		}

		@Override
		protected void map(ImmutableBytesWritable row, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// process data for the row from the Result instance.
			// String fromPaper = new
			// String(value.getValue(Bytes.toBytes("from"),
			// Bytes.toBytes("fromId")));
			if(value.getValue(Bytes.toBytes("community"), Bytes.toBytes("community")) != null) {
				String classes = Bytes.toString(value.getValue(Bytes.toBytes("community"), Bytes.toBytes("community")));
				if(!classes.equals("NIL") || !classes.trim().isEmpty() || classes.trim() != null) {
					int indegree = Integer
							.parseInt(new String(value.getValue(Bytes.toBytes("metricsIndirect"), Bytes.toBytes("indegree"))));
					//int indegree1 = Integer
					//		.parseInt(new String(value.getValue(Bytes.toBytes("metricsDirect"), Bytes.toBytes("indegree"))));

					int MEDIAN_INDEGREE = 4;  //add the median value here from the results of the PaperMetricMedian script
					Text TEXT_ONE = new Text("1");
					
					String comm[] = classes.trim().split(",");
					if (indegree > MEDIAN_INDEGREE) {
						for (String someclass : comm) {
							//System.out.println(someclass);
							if(!someclass.equals("NIL")) {
								context.write(new Text(someclass), TEXT_ONE);
							}
						}
					}
				}
			}

		}
	}

	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		Map<String, Integer> totalPaperCount = new HashMap<>();
		int totalPapers = 0;

		@Override
		protected void setup(Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "172.17.25.18");
			config.set("hbase.zookeeper.property.clientPort", "2183");

			HTable htableCommunityAbout = new HTable(config, "CommunityMetricAnalysis");
			
			Scan scan = new Scan();
			ResultScanner scanner = htableCommunityAbout.getScanner(scan);
			
			for (Result result = scanner.next(); (result != null); result = scanner.next()) {
				String className = Bytes.toString(result.getRow());
				String classCount = Bytes.toString(result.getValue(Bytes.toBytes("count"), Bytes.toBytes("count")));
			    totalPaperCount.put(className, Integer.parseInt(classCount));
			    totalPapers += Integer.parseInt(classCount);
			}
			
			
		}

		@Override
		protected void reduce(Text someClass, Iterable<Text> ValueOne, Context context)
				throws IOException, InterruptedException {

			double count = 0;
			for (Text One : ValueOne) {
				++count;
			}
			
		//	if(totalPaperCount.containsKey(someClass.toString().trim())){
				double fomd = count / totalPaperCount.get(someClass.toString());
				//double expansion = sum / totalPaperCount.get(forClass.toString());
				Put put = new Put(Bytes.toBytes(someClass.toString()));
				put.add(Bytes.toBytes("metricsIndirect"), Bytes.toBytes("FOMD"), Bytes.toBytes(fomd + ""));
				context.write(new ImmutableBytesWritable(Bytes.toBytes(someClass.toString())), put);
		//	}
				

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.18");
		conf.set("hbase.zookeeper.property.clientPort", "2183");

		Job job = new Job(conf, "FOMD");

		job.setJarByClass(MetricFOMDIndirect.class);
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