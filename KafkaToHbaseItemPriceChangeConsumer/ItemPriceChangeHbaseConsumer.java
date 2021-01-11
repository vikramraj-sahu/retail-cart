import java.nio.charset.Charset;
import java.util.Arrays;
import java.time.Duration;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class ItemPriceChangeHbaseConsumer
{
   public static void main(String[] args) throws Exception{
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "ip-20-0-31-4.ec2.internal:9092");
        consumerProps.put("group.id", "retail-price-group1");
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "300000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        
        final Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.master", "ip-20-0-21-196.ec2.internal:60020");
        hbaseConf.set("hbase.zookeeper.quorum","ip-20-0-21-196");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection hBase = ConnectionFactory.createConnection(hbaseConf);
        
        Table table = hBase.getTable(TableName.valueOf("edureka_788309_retailcart_price_change_events"));
    
        consumer.subscribe(Arrays.asList("edureka788309pricechange"));
        
        Long timeout = new Long(1000L);
        byte[] cf = "cf1".getBytes(Charset.forName("UTF-8"));
        
        String event_id = null; 
        String item_id = null; 
        String store_id = null; 
        String price_chng_activation_ts = null; 
        String geo_region_cd = null; 
        String price_change_reason = null; 
        String prev_price_amt = null; 
        String curr_price_amt = null; 
        String row_insertion_dttm = null;
        
        while (true) {
            
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeout));
            
            System.out.println("Total records:" + records.count());
             
            for (ConsumerRecord<String, String> record : records) {    
                String[] key = record.value().toString().split(",");
                
                System.out.println("Total Breaks:" + key.length);
                
                event_id = key[0];
                item_id = key[1];
                store_id = key[2];
                price_chng_activation_ts = key[3];
                geo_region_cd = key[4];
                price_change_reason = key[5];
                prev_price_amt = key[6];
                curr_price_amt = key[7];
                row_insertion_dttm = key[8];
                
                Put p = new Put(event_id.toString().getBytes(Charset.forName("UTF-8")));
                
                p.addColumn(cf,"event_id".getBytes(Charset.forName("UTF-8")), event_id.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"item_id".getBytes(Charset.forName("UTF-8")), item_id.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"store_id".getBytes(Charset.forName("UTF-8")), store_id.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"price_chng_activation_ts".getBytes(Charset.forName("UTF-8")), price_chng_activation_ts.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"geo_region_cd".getBytes(Charset.forName("UTF-8")), geo_region_cd.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"price_change_reason".getBytes(Charset.forName("UTF-8")), price_change_reason.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"prev_price_amt".getBytes(Charset.forName("UTF-8")), prev_price_amt.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"curr_price_amt".getBytes(Charset.forName("UTF-8")), curr_price_amt.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"row_insertion_dttm".getBytes(Charset.forName("UTF-8")), row_insertion_dttm.getBytes(Charset.forName("UTF-8")));
                table.put(p);
                
                System.out.println("Record Inserted into hbase: " + event_id);
            }
        }
    }

}