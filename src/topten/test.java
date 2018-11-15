package topten;

import java.lang.Comparable;
import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {
    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
		    String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
		    for (int i = 0; i < tokens.length - 1; i += 2) {
			String key = tokens[i].trim();
			String val = tokens[i + 1];
			map.put(key.substring(0, key.length() - 1), val);
		    }
		} catch (StringIndexOutOfBoundsException e) {
		    System.err.println(xml);
		}

		return map;
    }

	public static class Pair<T extends Comparable<T>, J> implements Comparable<Pair<T, J>> {

	  public T first;
	  public J second;

	  public Pair(T first, J second) {
	    this.first = first;
	    this.second = second;
	  }

	  public int compareTo(Pair<T, J> pair) {
	    return pair.first.compareTo(first);
	  }
	}

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    
			String [] rows = value.toString().split("\n");
			PriorityQueue<Pair<Integer, String>> sortedQueue = new PriorityQueue<Pair<Integer, String>>();
			
			for(String row: rows) {
				System.out.println(row);
				Map<String, String> map = transformXmlToMap(row);
				Integer rep = new Integer(map.get("reputation"));
				sortedQueue.add(new Pair<Integer, String>(rep, row));
			}

		    for(int i=0; i<10; i++) {
		    	Pair<Integer, String> next = sortedQueue.poll();
		    	if (next != null)
		    		context.write(NullWritable.get(), new Text(next.second.toString()));
		    	else
		    		break;
		    }
		}

    }

    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		    try {

		    	PriorityQueue<Pair<Integer, Integer>> sortedQueue = new PriorityQueue<Pair<Integer, Integer>>();
		    	for(Text text: values) {
		    		Map<String, String> map = transformXmlToMap(text.toString());
		    		Integer id = new Integer(map.get("id"));
		    		Integer rep  = new Integer(map.get("reputation"));
		    		sortedQueue.add(new Pair<Integer, Integer>(rep, id));
		    	}

				for(int i=0; i<10; i++) {
		    		Pair<Integer, Integer> next = sortedQueue.poll();
		    		if (next != null) {
		    			Put insHBase = new Put(Bytes.toBytes(next.second));
		    			insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sum"), Bytes.toBytes(next.first));
		    		}
		    		else
		    			break;
		    	}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

    public static void main(String[] args) throws Exception {
			Configuration conf = HBaseConfiguration.create();
			// define scan and define column families to scan
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes("cf"));
			Job job = Job.getInstance(conf, "topten");
			job.setJarByClass(TopTen.class);
			job.setMapperClass(TopTenMapper.class);
			job.setReducerClass(TopTenReducer.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
}
