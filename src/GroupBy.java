import java.io.IOException;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// EXAMPLE OF GROUP BY

// INPUT
//        ROW                                       COLUMN+CELL
//        k1                                        column=a:a, timestamp=1477737677673, value=25
//        k1                                        column=b:b, timestamp=1477737677868, value=10
//        k1                                        column=c:c, timestamp=1477737677958, value=300
//        k2                                        column=a:a, timestamp=1477737678038, value=50
//        k2                                        column=b:b, timestamp=1477737678119, value=10
//        k3                                        column=a:a, timestamp=1477737678162, value=10
//        k3                                        column=b:b, timestamp=1477737679554, value=20

// OUTPUT
//        ROW                                       COLUMN+CELL
//        b:10                                      column=a:a, timestamp=1477738460368, value=75
//        b:20                                      column=a:a, timestamp=1477738460368, value=10

public class GroupBy extends Configured implements Tool {
    public static final String PARAMETERS = "'inputTable outputTable aggregateAttribute groupByAttribute'";
    public static final String JOB_NAME = "GroupBy";
    public static final String ATTRIBUTES = "attributes";

    private static String inputTable;
    private static String outputTable;

    public static void main(String[] args) throws Exception {
        // Check the quantity of params received.
        if (args.length != 4) {
            System.err.println("Parameters missing: " + PARAMETERS);
            System.exit(1);
        }
        // Assign the input and output table to global variables.
        inputTable = args[0];
        outputTable = args[1];

        // Check the validity tables.
        int tablesRight = checkIOTables(args);
        if (tablesRight == 0) {
            // Execute the algorithm.
            int ret = ToolRunner.run(new GroupBy(), args);
            System.exit(ret);
        } else {
            System.exit(tablesRight);
        }
    }

    private static int checkIOTables(String [] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hba = new HBaseAdmin(config);

        // Check the existence of the input table.
        if (!hba.tableExists(inputTable)) {
            System.err.println("Input table does not exist");
            return 2;
        }
        // Check the nonexistence of the output table.
        if (hba.tableExists(outputTable)) {
            System.err.println("Output table already exists");
            return 3;
        }

        // Create the output table and assign the correspond family (aggregateAttribute).
        HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());
        htdOutput.addFamily(new HColumnDescriptor(args[2]));
        hba.createTable(htdOutput);

        return 0;
    }

    public int run(String [] args) throws Exception {
        // Create Configuration.
        Job job = new Job(HBaseConfiguration.create());
        job.setJarByClass(GroupBy.class);
        job.setJobName(JOB_NAME);

        // Create header and assign to configuration.
        String header;
        header = args[2] + "," + args[3];
        job.getConfiguration().setStrings(ATTRIBUTES, header);

        // Init map and reduce functions
        TableMapReduceUtil.initTableMapperJob(inputTable, new Scan(), Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 4;
    }

    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable rowMetadata, Result values, Context context) throws IOException, InterruptedException {

            String[] info = context.getConfiguration().getStrings(ATTRIBUTES, "empty");

            // Get the aggregate attribute and the attribute to group.
            String aggregateAttribute = info[0];
            String groupByAttribute = info[1];

            // Send the two attributes to combine, that reducer will receive.
            // The first element of 'getValue' function is the family and the second one is the qualifier.
            String agrValue = new String(values.getValue(aggregateAttribute.getBytes(), aggregateAttribute.getBytes()));
            String gBValue = new String(values.getValue(groupByAttribute.getBytes(), groupByAttribute.getBytes()));
            if (!agrValue.isEmpty() && !gBValue.isEmpty())
                context.write(new Text(gBValue), new Text(agrValue));

            // We want to obtain the values of the rows which have 'attributeValue' as a family
            // and 'attributeValue' as a qualifier.
            //      If we have <1, 2> and <1, 3> as a key-value pairs,
            //      we will obtain at the end of the combine function the following result: <1, {2, 3}>

        }
    }

    public static class Reducer extends TableReducer<Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {

            String[] info = context.getConfiguration().getStrings(ATTRIBUTES, "empty");

            // Get the aggregate attribute and the attribute to group.
            String aggregateAttribute = info[0];
            String groupByAttribute = info[1];

            int sum = 0;
            while (inputList.iterator().hasNext()) {
                // 'inputList' contains de combination of the key given in the map function.
                //      it would be {2, 3} if we considered the example above.
                // Iterate for all of them adding up the value for each.
                Text val = inputList.iterator().next();
                sum += Integer.parseInt(val.toString());
            }

            // Prepare the output row and then write it.
            String sumString = Integer.toString(sum);
            String outputTupleKey = groupByAttribute + ":" + key.toString();
            Put put = new Put(outputTupleKey.getBytes());
            put.add(aggregateAttribute.getBytes(), aggregateAttribute.getBytes(), sumString.getBytes());
            context.write(new Text(outputTupleKey), put);

        }
    }
}