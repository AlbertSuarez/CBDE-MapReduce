import java.io.IOException;
import java.util.Vector;
import java.util.ArrayList;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

// INPUT
//        ROW                                        COLUMN+CELL
//        k1                                         column=a:a, timestamp=1478040174459, value=25
//        k2                                         column=a:a, timestamp=1478040174522, value=50
//        k3                                         column=a:a, timestamp=1478040174550, value=10

//        ROW                                        COLUMN+CELL
//        k1                                         column=b:b, timestamp=1478040176161, value=70
//        k2                                         column=b:b, timestamp=1478040176219, value=50
//        k3                                         column=b:b, timestamp=1478040179596, value=15

// QUERY
// yarn jar RA.jar Join equipF_joinIN1 equipF_joinIN2 equipF_joinOUT a b

// OUTPUT
//        ROW                                        COLUMN+CELL
//        k2_k2                                      column=a:a, timestamp=1478040235460, value=50
//        k2_k2                                      column=b:b, timestamp=1478040235460, value=50

public class Join extends Configured implements Tool {

    public static final String PARAMETERS = "'leftInputTable rightInputTable outputTable leftAttribute rightAttribute'";
    public static final String JOB_NAME = "Join";
    public static final String FIRST_TABLE_ID = "External";
    public static final String SECOND_TABLE_ID = "Internal";
    public static final String LEFT_ATTRIBUTE_ID = "ExternalAttribute";
    public static final String RIGHT_ATTRIBUTE_ID = "InternalAttribute";
    public static final String HASH = "#";
    public static final String COLON = ":";
    public static final String SEMI_COLON = ";";
    public static final String UNDER_SCORE = "_";

    public static String inputTable1;
    public static String inputTable2;
    private static String outputTable;
    private static String leftAttribute;
    private static String rightAttribute;

    public static void main(String[] args) throws Exception {
        // Checking the parameter's count.
        if (args.length != 5) {
            System.err.println("Parameters missing: " + PARAMETERS);
            System.exit(1);
        }
        // Assigning the parameters to global variables.
        inputTable1 = args[0];
        inputTable2 = args[1];
        outputTable = args[2];
        leftAttribute = args[3];
        rightAttribute = args[4];

        // Checking the validity of the tables.
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

        // Check the existence of the first input table.
        if (!hba.tableExists(inputTable1)) {
            System.err.println("Input table 1 does not exist");
            return 2;
        }
        // Check the existence of the second input table.
        if (!hba.tableExists(inputTable2)) {
            System.err.println("Input table 2 does not exist");
            return 2;
        }
        // Check the nonexistence of the output table.
        if (hba.tableExists(outputTable)) {
            System.err.println("Output table already exists");
            return 3;
        }

        HTableDescriptor htdInput1 = hba.getTableDescriptor(inputTable1.getBytes());
        HTableDescriptor htdInput2 = hba.getTableDescriptor(inputTable2.getBytes());

        // Initialize the outputTable.
        HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());

        // Assign the same families that left input table.
        for (byte[] key: htdInput1.getFamiliesKeys())
            htdOutput.addFamily(new HColumnDescriptor(key));

        // Assign the same families that right input table.
        for (byte[] key: htdInput2.getFamiliesKeys())
            htdOutput.addFamily(new HColumnDescriptor(key));

        hba.createTable(htdOutput);
        return 0;
    }

    public int run(String [] args) throws Exception {
        // Create Configuration.
        Job job = new Job(HBaseConfiguration.create());
        job.setJarByClass(Join.class);
        job.setJobName(JOB_NAME);

        // To pass parameters to the mapper and reducer we must use the setStrings of the Configuration object.
        // We pass the names of two input tables as External and Internal tables of the Join operation,
        // and the two respective attributes to compare.
        job.getConfiguration().setStrings(FIRST_TABLE_ID, inputTable1);
        job.getConfiguration().setStrings(SECOND_TABLE_ID, inputTable2);
        job.getConfiguration().setStrings(LEFT_ATTRIBUTE_ID, leftAttribute);
        job.getConfiguration().setStrings(RIGHT_ATTRIBUTE_ID, rightAttribute);

        // To initialize the mapper, we need to provide two Scan objects as an array for two input tables, as follows.
        ArrayList<Scan> scans = new ArrayList<Scan>();

        Scan scan1 = new Scan();
        scan1.setAttribute("scan.attributes.table.name", Bytes.toBytes(inputTable1));
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.setAttribute("scan.attributes.table.name", Bytes.toBytes(inputTable2));
        scans.add(scan2);

        // Init map and reduce functions.
        TableMapReduceUtil.initTableMapperJob(scans, Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable rowMetadata, Result values, Context context) throws IOException, InterruptedException {

            String[] external = context.getConfiguration().getStrings(FIRST_TABLE_ID, "Default");
            String[] internal = context.getConfiguration().getStrings(SECOND_TABLE_ID, "Default");

            // From the context object we obtain the input TableSplit this row belongs to
            TableSplit currentSplit = (TableSplit)context.getInputSplit();

			// From the TableSplit object, we can further extract the name of the table that the split belongs to.
			// We use the extracted table name to distinguish between external and internal tables as explained below.
            TableName tableNameB = currentSplit.getTable();
            String tableName = tableNameB.getQualifierAsString();

            // We create a string as follows for each key: tableName#key;family:attributeValue.
            String tuple = tableName + HASH + new String(rowMetadata.get(), "US-ASCII");

            KeyValue[] attributes = values.raw();
            for (KeyValue attribute : attributes) {
                tuple += SEMI_COLON + new String(attribute.getFamily()) + COLON +
                        new String(attribute.getQualifier()) + COLON + new String(attribute.getValue());
            }

            // Is this key external (e.g., from the external table or first table)?
            if (tableName.equalsIgnoreCase(external[0])) {
                // If it is external, it gets as key a hash value and it is written only once in the context object
                context.write(new Text(Integer.toString(Double.valueOf(Math.random()*10).intValue())), new Text(tuple));
            }
            // Is this key internal (e.g., from the internal table or second table)?
            if (tableName.equalsIgnoreCase(internal[0])) {
                // If it is internal, it is written to the context object many times,
                // each time having as key one of the potential hash values
                for (int i = 0; i < 10; i++) {
                    context.write(new Text(Integer.toString(i)), new Text(tuple));
                }
            }
        }
    }

    public static class Reducer extends TableReducer<Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {
            String[] external = context.getConfiguration().getStrings(FIRST_TABLE_ID, "Default");
            String[] internal = context.getConfiguration().getStrings(SECOND_TABLE_ID, "Default");
            String[] leftAttribute = context.getConfiguration().getStrings(LEFT_ATTRIBUTE_ID, "Default");
            String[] rightAttribute = context.getConfiguration().getStrings(RIGHT_ATTRIBUTE_ID, "Default");

            // All tuple with the same hash value are stored in a vector.
            Vector<String> tupleVector = new Vector<String>();
            for (Text val : inputList)
                tupleVector.add(val.toString());

            // In this for, each internal tuple is joined with each external tuple.
            // Since the result must be stored in a HBase table, we configure a new Put,
            // fill it with the joined data and write it in the context object
            for (String eTableTuple : tupleVector) {
                // We extract the information from the tuple as we packed it in the mapper.
                String eTuple = eTableTuple.split(HASH)[1];
                String[] eAttributes = eTuple.split(SEMI_COLON);
                if (eTableTuple.startsWith(external[0])) {
                    for (String iTableTuple : tupleVector) {
                        // We extract the information from the tuple as we packed it in the mapper.
                        String iTuple = iTableTuple.split(HASH)[1];
                        String[] iAttributes = iTuple.split(SEMI_COLON);
                        if (iTableTuple.startsWith(internal[0])) {
                            // Create a key for the output.
                            String outputKey = eAttributes[0] + UNDER_SCORE + iAttributes[0];
                            // Create a tuple for the output table.
                            Put put = new Put(outputKey.getBytes());

                            String leftValue = null;
                            String rightValue = null;
                            // Set the values for the columns of the external table.
                            for (int k = 1; k < eAttributes.length; k++) {
                                String[] attribute_value = eAttributes[k].split(COLON);
                                if (attribute_value[1].equals(leftAttribute[0]))
                                    leftValue = attribute_value[2];
                                put.addColumn(attribute_value[0].getBytes(), attribute_value[1].getBytes(), attribute_value[2].getBytes());
                            }
                            // Set the values for the columns of the internal table.
                            for (int k = 1; k < iAttributes.length; k++) {
                                String[] attribute_value = iAttributes[k].split(COLON);
                                if (attribute_value[1].equals(rightAttribute[0]))
                                    rightValue = attribute_value[2];
                                put.addColumn(attribute_value[0].getBytes(), attribute_value[1].getBytes(), attribute_value[2].getBytes());
                            }
                            // Put the tuple in the output table through the context object.
                            if (leftValue != null && rightValue != null && leftValue.equals(rightValue))
                                context.write(new Text(outputKey), put);
                        }
                    }
                }
            }
        }
    }
}

