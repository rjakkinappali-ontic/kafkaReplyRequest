package com.example.kafkaReplyRequest.consumer;

import com.example.kafkaReplyRequest.Model.RequestModel;
import com.example.kafkaReplyRequest.Model.ResponseModel;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;



@Component
public class ReplyingKafkaConsumer {

    @KafkaListener(topics = "${kafka.topic.request-topic}")
    @SendTo
    public ResponseModel listen(RequestModel request) throws InterruptedException {

        String message = request.getMessage();
        ResponseModel response = new ResponseModel();
        response.setResponse("Got it: "+message);
        return response;
    }

}
