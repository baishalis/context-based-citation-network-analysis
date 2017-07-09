package mcad;

//update file...use Htable instead of HashSet

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
import java.util.HashSet;
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
import org.apache.hadoop.hbase.mapreduce.TableMapper;
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

public class ListKeywordsFromContext {
	public static class InputMapper extends TableMapper<Text, Text> {
		
		//HashMap<Text, Text> DOItoContext = new HashMap<>();
		//HashSet<String> globalKeyphraseSet = new HashSet();
		private HTable globalKeyphraseSet;
		private Configuration config;
		private HashMap<String, String> globalPhrases; 
		
		
		@Override
		protected void setup(
				org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "172.17.25.18");
			config.set("hbase.zookeeper.property.clientPort", "2183");
			globalPhrases = new HashMap<String, String>();
			try {
				globalKeyphraseSet = new HTable(config, "GlobalKeyphraseList");
				Scan scan = new Scan();
				ResultScanner scanner = globalKeyphraseSet.getScanner(scan);
				
				for (Result result = scanner.next(); result != null; result = scanner.next()) {
					byte[] rowkey = result.getRow();          
		            byte[] b_keyPhrases = result.getValue(Bytes.toBytes("keyphrase"), Bytes.toBytes("paperKeyphrases"));
		            String keyPhrases = Bytes.toString(b_keyPhrases);
		            String rowId = Bytes.toString(rowkey)
;					globalPhrases.put(rowId, keyPhrases);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			
			String DOI = Bytes.toString(row.get());
			
			String mergedContext = Bytes.toString(value.getValue(Bytes.toBytes("citationContext"), Bytes.toBytes("mergedContext")));
			
		//	String[] contexts = mergedContext.split(";;");
			
			String keyphrases = "";
			
//			Scan scan = new Scan();
//	        ResultScanner scanner = globalKeyphraseSet.getScanner(scan);
//	        Result r;
//	        
//	        while (((r = scanner.next()) != null)) {
//	            byte[] rowkey = r.getRow();          
//	            byte[] b_keyPhrases = r.getValue(Bytes.toBytes("keyphrase"), Bytes.toBytes("keyphrase"));
//	            String keyPhrases = Bytes.toString(b_keyPhrases);
//	            
//	            if(mergedContext.contains(keyPhrases.trim())) {
//					keyphrases += ";" + keyPhrases;
//				}
//					
//	        }
//	        
//	        scanner.close();
			
			Iterator it = globalPhrases.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		       // System.out.println(pair.getKey() + " = " + pair.getValue());
		        //String rowKey = pair.getKey().toString().trim();
		        String keyPhrases = pair.getValue().toString().trim();
		        String phrases[] = keyPhrases.split(";");
		        for(String phrase : phrases) {
		        	if(mergedContext.contains(phrase.trim())) {
						keyphrases += phrase + ";";
					}
		        }
		    }
	        System.out.println(DOI + " : " + keyphrases);
			context.write(new Text(DOI), new Text(keyphrases));
			
		}
	}
	
	
	public static class PaperEntryReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		@Override
		protected void reduce(Text doi, Iterable<Text> phrases, Context context)
				throws IOException, InterruptedException {

			String paperdoi = doi.toString().trim();
			String contextkeywordList = "";
			for (Text phrase : phrases) {
				contextkeywordList = phrase.toString();
			}
			
			Put put = new Put(Bytes.toBytes(paperdoi));
			put.add(Bytes.toBytes("termList"), Bytes.toBytes("termList"), Bytes.toBytes(contextkeywordList));
			
		//	System.out.println("r" + paperdoi + " : ");
			context.write(new ImmutableBytesWritable(Bytes.toBytes(paperdoi)), put);

		}
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.25.18");
		conf.set("hbase.zookeeper.property.clientPort", "2183");
		
		Job job = Job.getInstance(conf, "ListKeywordsFromContext");
		job.setJarByClass(ListKeywordsFromContext.class);
		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PaperEntryReducer.class);
		
		Scan scan = new Scan();
//		scan.setCaching(500);      
//		scan.setCacheBlocks(false); 
		scan.setStartRow(Bytes.toBytes("10.1.1.74.3662"));
//		 scan.setStopRow(Bytes.toBytes(""));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//FileInputFormat.setInputPaths(job, new Path(args[0]));	// "globalKeyphrases.txt"
		
		TableMapReduceUtil.initTableMapperJob("DOIMergedContext", scan, InputMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("DOIListTerms", PaperEntryReducer.class, job);
		
		job.setReducerClass(PaperEntryReducer.class);
		job.waitForCompletion(true);
	}

}