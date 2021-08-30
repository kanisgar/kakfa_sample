import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class kafkaMain {
    public void call_producer(JsonNode record)
    {
        Properties properties = new Properties();
        String bootStrapServer = "127.0.0.1:9092";
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    /*properties.put("bootstrap.servers", "localhost:9092");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");*/
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);
        String id = record.get("id").asText().trim();
        String name= record.get("name").asText().trim();
        StringBuffer sb = new StringBuffer();
        String finalResult = sb.append(id).append("|").append(name).toString();
        ProducerRecord<String, String> producer = new ProducerRecord<>("prabu", Integer.toString(1), finalResult);
        kafkaProducer.send(producer);
        kafkaProducer.close();
        System.out.println("Produced the record");
        //System.out.println("Calling the consumer");
        //call_consumer();
    }
    public void call_consumer()
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("kanisgar");
        kafkaConsumer.subscribe(topics);
        try{
            while (true){
                ConsumerRecords<String,String> records = kafkaConsumer.poll(10);
                for (ConsumerRecord<String,String> record: records){
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s, Offest is : %d", record.topic(), record.partition(), record.value(),record.offset()));
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode jso = objectMapper.readValue(record.value(),JsonNode.class);
                    System.out.println("The Record is " + jso );
                    call_producer(jso);
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }

public static void main(String args[])
{
    kafkaMain kafkaMain = new kafkaMain();

    kafkaMain.call_consumer();


    /*try{
        for(int i = 0; i < 100; i++){
            System.out.println(i);
            kafkaProducer.send(new ProducerRecord("demo", Integer.toString(i), "test message - " + i ));
        }
    }catch (Exception e){
        e.printStackTrace();
    }finally {
        kafkaProducer.close();
    }*/
}
}
