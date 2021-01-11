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


public class SalesTransactionHbaseConsumer {
    public static void main(String[] args) throws Exception {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "ip-20-0-31-4.ec2.internal:9092");
        consumerProps.put("group.id", "retail-group1");
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    
        System.out.println("Initiaization Completed ...");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        
        final Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.master", "ip-20-0-21-196.ec2.internal:60020");
        hbaseConf.set("hbase.zookeeper.quorum","ip-20-0-21-196");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection hBase = ConnectionFactory.createConnection(hbaseConf);
        Table table = hBase.getTable(TableName.valueOf("edureka_788309_retailcart_sales_transaction_events"));
    
        consumer.subscribe(Arrays.asList("edureka788309salestransaction"));
        
        Long timeout = new Long(1000L);
        byte[] cf = "cf1".getBytes(Charset.forName("UTF-8"));
        
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
        
        while (true) {
          
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeout));
            
            System.out.println("Total records:" + records.count());
            
            for (ConsumerRecord<String, String> record : records) {  
               
                String[] key = record.value().toString().split(",");
                
                System.out.println("Total Breaks:" + key.length);
            
                sales_id = key[0];
                store_id = key[1];
                geo_region_cd = key[4];
                item_id = key[2];
                scan_type = key[3];
                currency_code = key[5];
                scan_id = key[6];
                sold_unit_quantity = key[7];
                sales_timestamp = key[8];
                scan_dept_nbr = key[9];
                row_insertion_dttm = key[10];
                
                System.out.println( sales_id +"##"+ store_id +"##"+item_id+"##"+ scan_type +"##"+geo_region_cd  +"##"+currency_code +"##"+ scan_id +"##"+sold_unit_quantity +"##"+sales_timestamp +"##"+scan_dept_nbr +"##"+row_insertion_dttm );
                
                Put p = new Put(sales_id.toString().getBytes(Charset.forName("UTF-8")));


                p.addColumn(cf,"sales_id".getBytes(Charset.forName("UTF-8")), sales_id.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"store_id".getBytes(Charset.forName("UTF-8")), store_id.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"item_id".getBytes(Charset.forName("UTF-8")), item_id.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"scan_type".getBytes(Charset.forName("UTF-8")), scan_type.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"geo_region_cd".getBytes(Charset.forName("UTF-8")), geo_region_cd.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"currency_code".getBytes(Charset.forName("UTF-8")), currency_code.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"scan_id".getBytes(Charset.forName("UTF-8")), scan_id.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"sold_unit_quantity".getBytes(Charset.forName("UTF-8")), sold_unit_quantity.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"sales_timestamp".getBytes(Charset.forName("UTF-8")), sales_timestamp.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"scan_dept_nbr".getBytes(Charset.forName("UTF-8")), scan_dept_nbr.getBytes(Charset.forName("UTF-8")));
                p.addColumn(cf,"row_insertion_dttm".getBytes(Charset.forName("UTF-8")), row_insertion_dttm.getBytes(Charset.forName("UTF-8")));
               
               System.out.println("Record Inserted into hbase: " + sales_id);
               
               table.put(p);
            }
        }
    }
}
