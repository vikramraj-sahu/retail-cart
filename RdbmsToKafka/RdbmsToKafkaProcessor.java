/**
 * Main class for transferring mysql data to (Hadoop) Hbase in Real Time using Kafka
 */
public class RdbmsToKafkaProcessor {
   
   public static void main(String[] args) throws Exception{
       
       SalesTransactionProducer stProducer = new SalesTransactionProducer();
       ItemPriceChangeProducer ipcProducer = new ItemPriceChangeProducer();
       
       // Call To Producer Functions
       stProducer.copyMySqlRecordsToKafkaTopic();
       ipcProducer.copyMySqlRecordsToKafkaTopic();
   }

}