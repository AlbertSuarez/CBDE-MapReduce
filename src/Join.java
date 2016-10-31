import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// EXAMPLE OF JOIN

// INPUT

// QUERY

// OUTPUT

public class Join extends Configured implements Tool {
    public static final String PARAMETERS = "'leftInputTable rightInputTable outputTable leftAttribute rightAttribute'";
    public static final String JOB_NAME = "Join";
    public static final String ATTRIBUTES = "attributes";

    private static String inputTable1;
    private static String inputTable2;
    private static String outputTable;

    public static void main(String[] args) throws Exception {
        // Check the quantity of params received.
        if (args.length != 5) {
            System.err.println("Parameters missing: " + PARAMETERS);
            System.exit(1);
        }
        // Assign the input and output tables to global variables.
        inputTable1 = args[0];
        inputTable2 = args[1];
        outputTable = args[2];

        // Check the validity tables.
        int tablesRight = checkIOTables(args);
        if (tablesRight == 0) {
            // Execute the algorithm.
            int ret = ToolRunner.run(new Join(), args);
            System.exit(ret);
        } else {
            System.exit(tablesRight);
        }
    }

    private static int checkIOTables(String [] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hba = new HBaseAdmin(config);

        // Check the existence of the input table.
        if (!hba.tableExists(inputTable1)) {
            System.err.println("Input table 1 does not exist");
            return 2;
        }
        // Check the existence of the input table.
        if (!hba.tableExists(inputTable2)) {
            System.err.println("Input table 2 does not exist");
            return 2;
        }
        // Check the nonexistence of the output table.
        if (hba.tableExists(outputTable)) {
            System.err.println("Output table already exists");
            return 3;
        }

        // Initialize the outputTable
        HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());

        // Assign the same families that left input table.
        HTableDescriptor htdInput1 = hba.getTableDescriptor(inputTable1.getBytes());
        for (byte[] familyKey : htdInput1.getFamiliesKeys())
            htdOutput.addFamily(new HColumnDescriptor(familyKey));

        // Assign the same families that right input table.
        HTableDescriptor htdInput2 = hba.getTableDescriptor(inputTable2.getBytes());
        for (byte[] familyKey : htdInput2.getFamiliesKeys())
            htdOutput.addFamily(new HColumnDescriptor(familyKey));

        hba.createTable(htdOutput);
        return 0;
    }

    public int run(String [] args) throws Exception {
        // Create Configuration.
        Job job = new Job(HBaseConfiguration.create());
        job.setJarByClass(Join.class);
        job.setJobName(JOB_NAME);

        // Create header and assign to configuration.
        String header = args[3] + "," + args[4];
        job.getConfiguration().setStrings(ATTRIBUTES, header);

        ArrayList<Scan> scans = new ArrayList<Scan>();
        Scan scan1 = new Scan();
        scan1.setAttribute("scan.attributes.table.name", Bytes.toBytes(inputTable1));
        scans.add(scan1);
        Scan scan2 = new Scan();
        scan2.setAttribute("scan.attributes.table.name", Bytes.toBytes(inputTable2));
        scans.add(scan2);

        // Init map and reduce functions
        TableMapReduceUtil.initTableMapperJob(scans, Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 4;
    }

    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable rowMetadata, Result values, Context context) throws IOException, InterruptedException {

            String[] attributes = context.getConfiguration().getStrings(ATTRIBUTES, "empty");

        }
    }

    public static class Reducer extends TableReducer<Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {

            String[] attributes = context.getConfiguration().getStrings(ATTRIBUTES, "empty");

        }
    }
}