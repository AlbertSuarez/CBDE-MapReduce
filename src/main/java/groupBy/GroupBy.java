package groupBy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class GroupBy extends Configured implements Tool {

    public static final String ATTRIBUTES = "attributes";
    public static final String JOB_NAME = "GroupBy";
    public static final String ALREADY_EXISTS = "Output table already exists";
    public static final String NOT_EXIST = "Input table does not exist";
    public static final String PARAMETERS_MISSING = "Parameters missing: 'inputTable outputTable aggregateAttribute groupByAttribute'";

    private static String inputTable;
    private static String outputTable;

    public static void main(String[] args) throws Exception {
        // Check the quantity of params received.
        if (args.length != 4) {
            System.err.println(PARAMETERS_MISSING);
            System.exit(1);
        }
        // Assign the input and output table.
        inputTable = args[0];
        outputTable = args[1];

        // Check the validity tables.
        int tablesRight = checkIOTables(args);
        if (tablesRight == 0) {
            // Execute the algorithm.
            int ret = ToolRunner.run(new GroupBy(), args);
            System.exit(ret);
        }
        else
            System.exit(tablesRight);
    }

    private static int checkIOTables(String [] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hba = new HBaseAdmin(config);

        // Check the existence of the input table.
        if (!hba.tableExists(inputTable)) {
            System.err.println(NOT_EXIST);
            return 2;
        }
        // Check the nonexistence of the output table.
        if (hba.tableExists(outputTable)) {
            System.err.println(ALREADY_EXISTS);
            return 3;
        }

        // Create the output table and assign the correspond family (aggregateAttribute).
        HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());
        htdOutput.addFamily(new HColumnDescriptor(args[2]));

        hba.createTable(htdOutput);
        return 0;
    }

    public int run(String[] args) throws Exception {
        // Create Configuration.
        Job job = new Job(HBaseConfiguration.create());
        job.setJarByClass(GroupBy.class);
        job.setJobName(JOB_NAME);

        // Create scanner.
        Scan scan = new Scan();
        // Create header and assign to configuration.
        String header = args[2] + "," + args[3];
        job.getConfiguration().setStrings(ATTRIBUTES, header);

        // Init map and reduce functions
        TableMapReduceUtil.initTableMapperJob(inputTable, scan, Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 4;
    }

    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable rowMetadata, Result values, Context context) throws IOException, InterruptedException {
            String[] attributes = context.getConfiguration().getStrings(ATTRIBUTES, "empty");

            // Get the aggregate attribute and the attribute to group.
            String aggregate = attributes[0];
            String attribute = attributes[1];

            // Send the two attributes to combine, that reducer will receive.
            String aggregateValue = new String(values.getValue(aggregate.getBytes(), aggregate.getBytes()));
            String attributeValue = new String(values.getValue(attribute.getBytes(), attribute.getBytes()));
            if (!aggregateValue.isEmpty() && !attributeValue.isEmpty())
                context.write(new Text(attributeValue), new Text(aggregateValue));

        }

    }

    public static class Reducer extends TableReducer<Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {
            String[] attributes = context.getConfiguration().getStrings(ATTRIBUTES, "empty");

            // Get the aggregate attribute and the attribute to group.
            String aggregate = attributes[0];
            String attribute = attributes[1];
            int sum = 0;
            // 'inputList' contains de combination of the key given in the map function.
            // Iterate for all of them adding up the value for each.
            while (inputList.iterator().hasNext()) {
                Text val = inputList.iterator().next();
                sum += Integer.parseInt(val.toString());
            }

            // Prepare the output row and then write it.
            String outputKey = attribute + ":" + key.toString();
            Put put = new Put(outputKey.getBytes());
            put.add(aggregate.getBytes(), aggregate.getBytes(), Integer.toString(sum).getBytes());
            context.write(new Text(outputKey), (Writable) put);
        }

    }
}