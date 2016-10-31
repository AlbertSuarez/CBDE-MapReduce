import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
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
import java.io.IOException;

// EXAMPLE OF SELECTION

// INPUT

// QUERY

// OUTPUT

public class Selection extends Configured implements Tool {
    public static final String PARAMETERS = "'inputTable outputTable [family:]attribute value'";
    public static final String JOB_NAME = "Selection";
    public static final String ATTRIBUTES = "attributes";
    public static final String COLON = ":";
    public static final String SEMI_COLON = ";";

    private static String inputTable;
    private static String outputTable;
    private static String attribute;
    private static String value;

    public static void main(String[] args) throws Exception {
        // Check the quantity of params received.
        if (args.length != 4) {
            System.err.println("Parameters missing: " + PARAMETERS);
            System.exit(1);
        }
        // Assign the input, output table, attribute and value to global variables.
        inputTable = args[0];
        outputTable = args[1];
        attribute = args[2];
        value = args[3];

        // Check the validity tables.
        int tablesRight = checkIOTables(args);
        if (tablesRight == 0) {
            // Execute the algorithm.
            int ret = ToolRunner.run(new Selection(), args);
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

        // Create the output table and assign the same families as input table.
        HTableDescriptor htdInput = hba.getTableDescriptor(inputTable.getBytes());
        HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());
        for (byte[] familyKey : htdInput.getFamiliesKeys())
            htdOutput.addFamily(new HColumnDescriptor(familyKey));
        hba.createTable(htdOutput);

        return 0;
    }

    public int run(String [] args) throws Exception {
        // Create Configuration.
        Job job = new Job(HBaseConfiguration.create());
        job.setJarByClass(Selection.class);
        job.setJobName(JOB_NAME);

        // Create header and assign to configuration.
        Scan scan = new Scan();
        String header = attribute + "," + value;
        job.getConfiguration().setStrings(ATTRIBUTES, header);

        // Init map and reduce functions
        TableMapReduceUtil.initTableMapperJob(inputTable, scan, Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 4;
    }

    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable rowMetadata, Result values, Context context) throws IOException, InterruptedException {

            String rowId = new String(rowMetadata.get(), "US-ASCII");
            String[] attributes = context.getConfiguration().getStrings(ATTRIBUTES, "empty");

            // Get the attribute and the value to select.
            String attribute = attributes[0];
            String value = attributes[1];

            // Assign to 'attributeValue' the value of the attribute to compare with the selection value
            String attributeValue;
            if (attribute.contains(COLON)) {
                String[] split = attribute.split(COLON);
                attributeValue = new String(values.getValue(split[0].getBytes(), split[1].getBytes()));
            }
            else
                attributeValue = new String(values.getValue(attribute.getBytes(), attribute.getBytes()));

            // If the value is equals, write the content of the key to context
            if (attributeValue.equals(value)) {
                KeyValue[] raw = values.raw();
                String tuple = new String(raw[0].getFamily()) + COLON + new String(raw[0].getValue());
                for (int i = 1; i < raw.length; i++)
                    tuple += SEMI_COLON + new String(raw[i].getFamily()) + COLON + new String(raw[i].getValue());
                // Send to reducer tuple identified by the 'rowId'
                context.write(new Text(rowId), new Text(tuple));
            }

            // In conclusion, we send to reducer the rows that attribute value is equals
            // with value received as a parameter
        }
    }

    public static class Reducer extends TableReducer<Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {

            while (inputList.iterator().hasNext()) {
                // 'inputList' contains de rows that achieve the condition.
                // Iterate for all of them adding in the output table.
                Text outputKey = inputList.iterator().next();
                Put put = new Put(key.getBytes());
                for (String row : key.toString().split(SEMI_COLON)) {
                    String[] values = row.split(COLON);
                    // Adding the family, qualifier and the value respectively.
                    put.add(values[0].getBytes(), values[0].getBytes(), values[1].getBytes());
                }
                // Write to output table.
                context.write(outputKey, put);
            }
        }
    }
}