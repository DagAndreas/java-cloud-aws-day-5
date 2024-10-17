package com.booleanuk.OrderService.controllers;


import com.booleanuk.OrderService.models.Order;
import com.booleanuk.OrderService.repositories.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;

@RestController
@RequestMapping("orders")
public class OrderController {
    private SqsClient sqsClient;
    private SnsClient snsClient;
    private EventBridgeClient eventBridgeClient;
    private ObjectMapper objectMapper;
    private String queueUrl;
    private String topicArn;
    private String eventBusName;

    private final OrderRepository orderRepository;

    public OrderController(OrderRepository orderRepository) {
        this.sqsClient = SqsClient.builder().build();
        this.snsClient = SnsClient.builder().build();
        this.eventBridgeClient = EventBridgeClient.builder().build();

        this.queueUrl = "https://sqs.eu-west-1.amazonaws.com/637423341661/dagOrderQueue";
        this.topicArn = "arn:aws:sns:eu-west-1:637423341661:dagOrderCreatedTopic";
        this.eventBusName = "dagCustomEventBus";

        this.objectMapper = new ObjectMapper();

        this.orderRepository = orderRepository;
    }


    @GetMapping
    public ResponseEntity<String> GetAllOrders() {
        // bygg den request: hent alle orders
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                // fra denne url'en
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20)
                .build();

        // hent messages fra client fra requesten vi bygde
        List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

        // for alle messages:
        for (Message message : messages) {
            try {
                // map en json order til Order order
                Order order = this.objectMapper.readValue(message.body(), Order.class);
                //TODO: fix processOrder
                this.processOrder(order);
                System.out.println(order);

                // slett en order etter at vi har processert den
                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();

                sqsClient.deleteMessage(deleteRequest);
            } catch (JsonProcessingException e) {

                System.out.println(message.body());
                e.printStackTrace();
            }
        }
        for(Message message : messages){
            System.out.println(message.toString());
        }
        String status = String.format("%d Orders have been processed", messages.size());
        return ResponseEntity.ok(status);
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody Order order) {
        try {
            String orderJson = objectMapper.writeValueAsString(order);
            System.out.println(orderJson);
            PublishRequest publishRequest = PublishRequest.builder()
                    .topicArn(topicArn)
                    .message(orderJson)
                    .build();
            snsClient.publish(publishRequest);

            PutEventsRequestEntry eventEntry = PutEventsRequestEntry.builder()
                    .source("order.service")
                    .detailType("OrderCreated")
                    .detail(orderJson)
                    .eventBusName(eventBusName)
                    .build();

            PutEventsRequest putEventsRequest = PutEventsRequest.builder()
                    .entries(eventEntry)
                    .build();

            this.eventBridgeClient.putEvents(putEventsRequest);

            String status = "Order created, Message Published to SNS and Event Emitted to EventBridge";
            return ResponseEntity.ok(status);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            System.out.println("Error when creating");
            return ResponseEntity.status(500).body("Failed to create order");
        }
    }

    //TODO: fix processOrder
    private void processOrder(Order order) {
        System.out.println(order.toString());
    }
}
