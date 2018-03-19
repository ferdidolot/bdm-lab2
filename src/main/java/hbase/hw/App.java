package hbase.hw;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
/**
 * Hello world!
 *
 */
public class App 
{	
	Configuration config;
	TableName table1 = TableName.valueOf("Table1");
	String family1 = "Family1";
	String family2 = "Family2";
	final byte[] qualifier1 = Bytes.toBytes("Qualifier1");
	Admin admin;
	Connection connection;
	
	private void connect() {
		config = HBaseConfiguration.create();
        String path = this.getClass()
          .getClassLoader()
          .getResource("hbase-site.xml")
          .getPath();
        config.addResource(new Path(path));
		System.out.println("Connected to HBase");
	}
	
	private void createTable() throws Exception {
		connection = ConnectionFactory.createConnection(config);
		admin = connection.getAdmin();
		HTableDescriptor desc = new HTableDescriptor(table1);
		desc.addFamily(new HColumnDescriptor(family1));
		desc.addFamily(new HColumnDescriptor(family2));
		admin.createTable(desc);
		System.out.println("Table created");
	}
	
	private void addRow() throws Exception{
		byte[] row1 = Bytes.toBytes("row1");
		Put p = new Put(row1);
		p.addImmutable(family1.getBytes(), qualifier1, Bytes.toBytes("cell_data"));
		connection.getTable(table1).put(p);
		System.out.println("Row added");
	}
	
    public static void main( String[] args ) throws Exception{
    	App app = new App();
    	app.connect();
    	app.createTable();
    	app.addRow();
        System.out.println( "Hello World!" );
        
    }
}
