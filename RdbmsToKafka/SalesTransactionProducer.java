/**
 * Producer Class for Sales Item Transaction
 */
import java.util.Properties;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SalesTransactionProducer {
    private static KafkaProducer<String, String> producer;

    private static final String topic = "edureka788309salestransaction";

    SalesTransactionProducer() throws Exception {

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "ip-20-0-31-221.ec2.internal:9092");
        producerProps.put("acks", "all");
        producerProps.put("retries", 0);
        producerProps.put("batch.size", 16384);
        producerProps.put("linger.ms", 1);
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String,String>(producerProps);
    }

    public void copyMySqlRecordsToKafkaTopic() throws Exception {
                 
        String msg = null;
        String sales_id =  null;
        String store_id =  null;
        String item_id =  null;
        String scan_type =  null;
        String geo_region_cd =  null;
        String currency_code = null;
        String scan_id =  null;
        String sold_unit_quantity =  null;
        String sales_timestamp =  null;
        String scan_dept_nbr = null;
        String row_insertion_dttm =  null;
        
        String myDriver = "com.mysql.jdbc.Driver";
        String myUrl = "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database";
        Class.forName(myDriver);
        Connection conn = DriverManager.getConnection(myUrl,"edu_labuser", "edureka");
        String query = "SELECT * FROM edureka_788309_retailcart_sales_transaction_events";
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(query);
        while (rs.next())
        {
            sales_id = rs.getString("sales_id");
            store_id = rs.getString("store_id");
            item_id = rs.getString("item_id");
            scan_type = rs.getString("scan_type");
            geo_region_cd = rs.getString("geo_region_cd");
            currency_code = rs.getString("currency_code");
            scan_id = rs.getString("scan_id");
            sold_unit_quantity = rs.getString("sold_unit_quantity");
            sales_timestamp = rs.getString("sales_timestamp");
            scan_dept_nbr = rs.getString("scan_dept_nbr");
            row_insertion_dttm = rs.getString("row_insertion_dttm");
            
            msg = sales_id+","+store_id+","+item_id+","+scan_type+","+geo_region_cd+","+currency_code+","+scan_id+","+sold_unit_quantity+","+sales_timestamp+","+
            scan_dept_nbr+","+row_insertion_dttm ;
            ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic,msg);
            producer.send(producerRecord);
        }
        conn.close();
    }
}