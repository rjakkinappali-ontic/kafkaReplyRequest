package com.example.kafkaReplyRequest.controller;

import com.example.kafkaReplyRequest.Model.RequestModel;
import com.example.kafkaReplyRequest.Model.ResponseModel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/testing")
public class KafkaTestController {

    @Autowired
    ReplyingKafkaTemplate<String, RequestModel, ResponseModel> kafkaTemplate;

    @Value("${kafka.topic.request-topic}")
    String requestTopic;

    @Value("${kafka.topic.requestreply-topic}")
    String requestReplyTopic;

    @ResponseBody
    @PostMapping(value="/sum")
    public ResponseModel sum(@RequestParam(value = "message") String message) throws InterruptedException, ExecutionException {
        RequestModel request = new RequestModel();
        request.setMessage(message);
        // create producer record
        ProducerRecord<String, RequestModel> record = new ProducerRecord<String, RequestModel>(requestTopic, request);
        // set reply topic in header
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic.getBytes()));
        // post in kafka topic
        RequestReplyFuture<String, RequestModel, ResponseModel> sendAndReceive = kafkaTemplate.sendAndReceive(record);

        // confirm if producer produced successfully
        SendResult<String, RequestModel> sendResult = sendAndReceive.getSendFuture().get();

        //print all headers
        sendResult.getProducerRecord().headers().forEach(header -> System.out.println("header value: "+header.key() + ":" + header.value().toString()));

        // get consumer record
        ConsumerRecord<String, ResponseModel> consumerRecord = sendAndReceive.get();
        // return consumer value
        return consumerRecord.value();
    }

}