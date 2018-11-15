package topten;

import java.io.IOException;
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

    // Pair which have a natural order based on the first element
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
    // Stores a queue of user reputation to the record
        private PriorityQueue<Pair<Integer, String>> sortedQueue = new PriorityQueue<Pair<Integer, String>>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            toRecordMap(value, repToRecordMap);

            if (repToRecordMap.size() > 10) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text val : repToRecordMap.values()) {
                context.write(NullWritable.get(), new Text(val));
            }
        }
    }

    public static class TopTenReducer extends TableReducer<NullWritable, Text, ImmutableBytesWritable> {
    // Stores a queue of user reputation to the record
        private PriorityQueue<Pair<Integer, Integer>> sortedQueue = new PriorityQueue<Pair<Integer, Integer>>();

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                toRecordMap(val, repToRecordMap);
                if (repToRecordMap.size() > 10) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }

            
            for (Map.Entry<Integer,Text> entry : repToRecordMap.descendingMap().entrySet()) {
                Integer keyMap = entry.getKey();
                String valMap = entry.getValue().toString();
                
                Put insHBase = new Put(Bytes.toBytes(valMap));

                insHBase.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(valMap));
                insHBase.add(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(keyMap));

                context.write(null, insHBase);
            }
        }

    }

    public static void toRecordMap(Text value, TreeMap<Integer, Text> repToRecordMap) throws NumberFormatException {
        Map<String, String> parsedMap = transformXmlToMap(value.toString().trim());

        Integer rowId = parsedMap.get("Id") != null ? Integer.parseInt(parsedMap.get("Id")) : null;
        if (rowId != null && rowId > 0) {
            if (parsedMap.get("Reputation") != null) {
                Integer reputation = Integer.parseInt(parsedMap.get("Reputation"));
                repToRecordMap.put(reputation, new Text(value));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            Configuration conf = HBaseConfiguration.create();;
            Job job = Job.getInstance(conf, "topten");
            job.setJarByClass(TopTen.class);
            
            job.setMapperClass(TopTenMapper.class);
            job.setReducerClass(TopTenReducer.class);

            job.setNumReduceTasks(1);

            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);
            
            
            FileInputFormat.addInputPath(job, new Path(args[0]));
            
            TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);
            
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception ex) {
            System.out.println("Main Error: " + ex.getMessage());
        }
    }
}
