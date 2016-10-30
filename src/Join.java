import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class Join extends Configured implements Tool {

      public static void main(String[] args) throws Exception { 
        if (args.length<5) {
          System.err.println("Parameters missing: ");
          System.exit(1);
        }
      }

    public int run(String[] strings) throws Exception {
        return 0;
    }
}