package com.excelsys.morphlines;


import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;


/**
 * Hello world!
 */
public class StreamingApp {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        File configFile = new File("morphline.conf");
        MorphlineContext context = new MorphlineContext.Builder().build();
        Command morphline = new Compiler().compile(configFile, null, context, null);

        // process each input data file
        Notifications.notifyBeginTransaction(morphline);

        AmazonSQS c = new AmazonSQSClient();
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();

        receiveMessageRequest.setMaxNumberOfMessages(10);
        receiveMessageRequest.setQueueUrl("https://sqs.us-east-1.amazonaws.com/235368163414/aldrinleal-tweets-queue");

        while (true) {
            ReceiveMessageResult receiveMessageResult = c.receiveMessage(receiveMessageRequest);

            for (Message m : receiveMessageResult.getMessages()) {

                ObjectNode objNode = (ObjectNode) OBJECT_MAPPER.readTree(m.getBody());

                InputStream payload = new ByteArrayInputStream(objNode.get("Message").textValue().getBytes());

                Record record = new Record();
                record.put(Fields.ATTACHMENT_BODY, payload);
                boolean success = morphline.process(record);

                if (success)
                    c.deleteMessage("https://sqs.us-east-1.amazonaws.com/235368163414/aldrinleal-tweets-queue", m.getReceiptHandle());
            }
        }

        //Notifications.notifyCommitTransaction(morphline);
    }
}
