package com.dgtz.api.queue;

import com.dgtz.api.queue.services.LiveStreamDebateDoneFactory;
import com.dgtz.api.queue.services.LiveStreamDoneFactory;
import com.dgtz.api.queue.services.LiveStreamThumbnailFactory;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * BroCast.
 * Copyright: Sardor Navruzov
 * 2013-2016.
 */

@Component
public class RabbitMqListener {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RabbitMqListener.class);

    @RabbitListener(queues = "liveQ")
    public void processQueue1(String message) throws Exception {
        logger.info("Received from LIVE queue 1: " + message);
        sendToProc(message);
    }

    @RabbitListener(queues = "liveQ")
    public void processQueue2(String message) throws Exception {
        logger.info("Received from LIVE queue 2: " + message);
        sendToProc(message);
    }

    @RabbitListener(queues = "liveDQ")
    public void processDQueue1(String message) throws Exception {
        logger.info("Received from Dqueue 1: " + message);
        sendToDebateProc(message);
    }

    @RabbitListener(queues = "liveDQ")
    public void processDQueue2(String message) throws Exception {
        logger.info("Received from Dqueue 2: " + message);
        sendToDebateProc(message);
    }

    @RabbitListener(queues = "liveThumbQ")
    public void processThumbQueue1(String message) throws Exception {
        logger.info("Received from thumb queue 1: " + message);
        sendToThumbProc(message);
    }

    @RabbitListener(queues = "liveThumbQ")
    public void processThumbQueue2(String message) throws Exception {
        logger.info("Received from thumb queue 2: " + message);
        sendToThumbProc(message);
    }

    private void sendToProc(String message) throws Exception {
        //live_id16342_43380
        String[] lvStr = message.split("∞");
        String stream = "live_id"+lvStr[0]+"_"+lvStr[1];
        String app = lvStr[2];
        Integer rotation = Integer.valueOf(lvStr[4]);
        Long idUser = Long.valueOf(lvStr[0]);
        Long idLive = Long.valueOf(lvStr[1]);
        String streamFull = lvStr[3];

        LiveStreamDoneFactory factory =
                new LiveStreamDoneFactory(stream,app,idUser,idLive, streamFull, rotation);
        factory.doneIt();
    }

    private void sendToDebateProc(String message) throws Exception {
        //live_id16342_43380
        String[] lvStr = message.split("∞");
        String stream = "live_id"+lvStr[0]+"_"+lvStr[1];
        Long idUser = Long.valueOf(lvStr[0]);
        Long idLive = Long.valueOf(lvStr[1]);

        LiveStreamDebateDoneFactory factory =
                new LiveStreamDebateDoneFactory(stream,idUser,idLive);
        factory.doneIt();
    }

    private void sendToThumbProc(String message) throws Exception {
        String[] lvStr = message.split("∞");
        String path = lvStr[2];
        Long idUser = Long.valueOf(lvStr[0]);
        Long idLive = Long.valueOf(lvStr[1]);
        Integer rotation = Integer.valueOf(lvStr[3]);

        LiveStreamThumbnailFactory factory =
                new LiveStreamThumbnailFactory(path,idLive,idUser, rotation);
        factory.doThumb();
    }
}
