package com.dgtz.api.queue.utils;

import com.dgtz.api.enums.EnumErrors;
import com.dgtz.api.feature.AmazonS3Module;
import com.dgtz.mcache.api.factory.Constants;
import com.dgtz.mcache.api.factory.RMemoryAPI;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Digital Citizen.
 * User: Sardor Navuzov
 * Date: 3/9/15
 */
public class HLSUploadModule {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(HLSUploadModule.class);

    public HLSUploadModule() {
    }

    public static EnumErrors uploadHLS(Long idUser, Long idLive, String hlsSource, AmazonS3Module s3Module, String streamName) throws Exception {

/*
        while (!FileUtil.isFileFreeFromFFMPEG(hlsSource + "index.m3u8")) {
            log.info("Waiting for FFMPEG Finishing");
            Thread.sleep(2000);
        }
*/
        //modifyHLSFile(hlsSource + "index.m3u8");
        String dest = Constants.encryptAmazonURL(idUser, idLive, "", "hls", "");
        dest = dest.substring(dest.lastIndexOf("/")+1);
        log.info("HLS LIVE URL: {}", dest);
        FileUtil.copyFile(hlsSource + "index.m3u8", hlsSource + dest);

        /*HLS has been built and uploaded*/
        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + idLive, "temp_hls"
                , Constants.HLS_URL + streamName+"/index.m3u8");
        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "hls:" + idLive, "status", "done");
        RMemoryAPI.getInstance().pushElemToMemory(Constants.MEDIA_KEY + "encstate:" + idLive, 3, "100");

        return s3Module.uploadHLSFolder(idLive, idUser + "/hls_vod/" + idLive + "/", hlsSource);
    }

    public static void prepLocalHLS(Long idLive, String hlsSource, String streamName) throws IOException, InterruptedException {
        //modifyHLSFile(hlsSource + "index.m3u8");

        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY+idLive, "temp_hls"
                , Constants.HLS_URL + streamName+"/index.m3u8");

        /*HLS has been built and uploaded*/
        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "hls:" + idLive, "status", "done");
        RMemoryAPI.getInstance().pushElemToMemory(Constants.MEDIA_KEY + "encstate:" + idLive, 3, "100");

        //FileUtil.processHlsToMp4(streamName, idLive);

    }

    private static void modifyHLSFile(String filePath) {
        String s = "#EXT-X-ENDLIST";
        byte data[] = s.getBytes();
        Path p = Paths.get(filePath);

        try (OutputStream out = new BufferedOutputStream(
                Files.newOutputStream(p, StandardOpenOption.WRITE, StandardOpenOption.APPEND))) {
            out.write(data, 0, data.length);
        } catch (Exception x) {
            log.error("ERROR IN WR HLS MODUL", x);
        }
    }
}
