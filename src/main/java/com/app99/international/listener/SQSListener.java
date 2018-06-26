package com.app99.international.listener;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.app99.international.model.Functions;
import com.app99.international.service.ImpalaService;
import com.jayway.jsonpath.JsonPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Component
public class SQSListener implements MessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQSListener.class);

    @Autowired
    private ImpalaService impalaService;

    @Autowired
    private JmsTemplate jmsTemplate;



    public void onMessage(Message message) {
        TextMessage textMessage = (TextMessage) message;
        try {
            String fileKey = JsonPath.read(textMessage.getText(), "$.Records[0].s3.object.key");
            String bucketName = JsonPath.read(textMessage.getText(), "$.Records[0].s3.bucket.name");

            if (fileKey.endsWith(".gz")) {
                LOGGER.info("Json file Key got from JSON = " + bucketName + "/" + fileKey);
                LOGGER.info("Received message " + textMessage.getText());

                String[] token = fileKey.split("/");
                String tableName = token[2];

                if (tableName.equals("backfill")){
                    impalaService.executeQuery(impalaService.backfillTable(token[3]));
                }else {
                    String year   = token[3];
                    String month  = token[4];
                    String day    = token[5];
                    String hour   = token[6];

                    LOGGER.info("===================================== " + tableName);
                    if(impalaService.executeQuery(impalaService.AddPartitionsS3("redshift", tableName, new String[]{year, month, day, hour}))) {
                        if(Functions.startJob(day + "-" + month + "-" + year)){
                            impalaService.executeQuery(impalaService.prepareCommand("redshift", "new_app", tableName, year, month, day, hour));
                        }
                    } else {
                        LOGGER.warn("Table: " + tableName + " - ERROR When was add a new partition " + fileKey);
                    }
                }
            } else {
                LOGGER.warn("The message received was not processed because it is not a processable gz file. " + textMessage.getText());
            }
        } catch (AmazonS3Exception ae) {
            if (ae.getErrorCode().equals("NoSuchKey") && ae.getErrorResponseXml().contains("hive-export/queries/rides")) {
                LOGGER.error("NoSuchKey Exception processing the files. Sending message to the queue again to wait for the ride file");
                jmsTemplate.convertAndSend(message);
            }
        } catch(ParseException pe){
            LOGGER.warn("Invalid date. " + pe.getMessage());
        }catch (Exception e) {
            LOGGER.error("Unknown error processing message, ignoring message. " + e.getMessage());
        }
    }

}