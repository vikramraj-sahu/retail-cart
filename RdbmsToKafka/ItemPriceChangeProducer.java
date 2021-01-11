/**
 * Producer Class for Item Price Change
 */
import java.util.Properties;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ItemPriceChangeProducer {
    private static KafkaProducer<String, String> producer;
    private static final String topic = "edureka788309pricechange";

    ItemPriceChangeProducer() {
        Properties producerProps = new Properties();
       
        // Set Producer Properties
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
        String event_id = null; 
        String item_id = null; 
        String store_id = null; 
        String price_chng_activation_ts = null; 
        String geo_region_cd = null; 
        String price_change_reason = null; 
        String prev_price_amt = null; 
        String curr_price_amt = null; 
        String row_insertion_dttm = null;
        
        String myDriver = "com.mysql.jdbc.Driver";
        String myUrl = "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database";
        Class.forName(myDriver);
        Connection conn = DriverManager.getConnection(myUrl,"edu_labuser", "edureka");
        String query = "SELECT * FROM edureka_788309_retailcart_price_change_events";
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(query);
        
        while (rs.next()) {
            
            event_id = rs.getString("event_id");
            item_id = rs.getString("item_id");
            store_id = rs.getString("store_id");
            price_chng_activation_ts = rs.getString("price_chng_activation_ts");
            geo_region_cd = rs.getString("geo_region_cd");
            price_change_reason = rs.getString("price_change_reason");
            prev_price_amt = rs.getString("prev_price_amt");
            curr_price_amt = rs.getString("curr_price_amt");
            row_insertion_dttm = rs.getString("row_insertion_dttm");

            msg = event_id+","+item_id+","+store_id+","+price_chng_activation_ts+","+geo_region_cd+","+price_change_reason+","+prev_price_amt+","+curr_price_amt+","+row_insertion_dttm;
            ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic,msg);
            producer.send(producerRecord);
        }
        conn.close();
    }
}