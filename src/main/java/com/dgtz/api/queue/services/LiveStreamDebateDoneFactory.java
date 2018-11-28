package com.dgtz.api.queue.services;

import com.dgtz.api.beans.MediaInfo;
import com.dgtz.api.enums.EnumErrors;
import com.dgtz.api.feature.AmazonS3Module;
import com.dgtz.api.feature.SaveOriginal;
import com.dgtz.api.queue.utils.FileUtil;
import com.dgtz.api.queue.utils.HLSUploadModule;
import com.dgtz.api.queue.utils.MediaSettings;
import com.dgtz.api.settings.IFileManipulate;
import com.dgtz.mcache.api.factory.Constants;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * BroCast.
 * Copyright: Sardor Navruzov
 * 2013-2017.
 */
public class LiveStreamDebateDoneFactory {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(LiveStreamDebateDoneFactory.class);
    private static Long durationLimit = 7200l;

    private String stream;

    private Long idUser = 0L;
    private Long idLive = 0L;
    private String videoSource;
    private String hlsSource;


    public LiveStreamDebateDoneFactory(String stream, Long idUser, Long idLive) {
        this.stream = stream;
        this.idUser = idUser;
        this.idLive = idLive;
        this.videoSource = "/opt/dump/live/rec/" + stream + ".mp4";
        this.hlsSource = "/opt/hls_fragments/hls_pro/" + stream + "/";
    }

    public void doneIt() throws Exception {
        EnumErrors errors = EnumErrors.NO_ERRORS;
        try {

            if (idLive > 0 && idUser >= 0) {

                log.info("UPDATE LIVE Debate PROCESS, DEL FROM LIST {}", idLive);
                FileUtil.mountStorageDisk();
                AmazonS3Module s3Module = new AmazonS3Module();
                String hls_url = Constants.encryptAmazonURL(idUser, idLive, "", "hls", "");
                String mp4_url = Constants.encryptAmazonURL(idUser, idLive, "_hi.mp4", "v", "");

                boolean isRecordHLSExists = Files.exists(Paths.get(hlsSource + "index.m3u8"));

                log.info("Moving tp pro directory {}", hlsSource);
                /*Files.move(new File(hlsSource).toPath(),
                           new File("/opt/hls_fragments/hls_pro/" + stream).toPath(), StandardCopyOption.REPLACE_EXISTING);*/

                hlsSource = "/opt/hls_fragments/hls_pro/" + stream + "/";

                if (isRecordHLSExists) {
                    HLSUploadModule.prepLocalHLS(idLive, hlsSource, stream);
                    String localVideoSrc = "/opt/dump/" + System.currentTimeMillis() + ".mp4";
                    FileUtil.copyFile(videoSource, localVideoSrc);

                    hls_url = hls_url.substring(hls_url.lastIndexOf("/")+1);
                    log.info("HLS DEBATE LIVE URL: {}", hls_url);

                    FileUtil.copyFile(hlsSource + "index.m3u8", hlsSource + hls_url);
                    IFileManipulate fileManipulate = new SaveOriginal();

                    MediaInfo mediaInfo = fileManipulate.info(hlsSource+"index.m3u8", com.dgtz.api.enums.EnumFileType.VIDEO);
                    s3Module.uploadHLSFolder(idLive, idUser + "/hls_vod/" + idLive + "/", hlsSource);
                    if (mediaInfo != null) {
                        MediaSettings settings = new MediaSettings();
                        settings.saveVideoProperties(idLive, mediaInfo, false);
                    }
                    errors = s3Module.uploadOriginalFile(mp4_url, localVideoSrc, 0);
                } else {
                    log.info("File is not recorded IDUSER " + idUser + " idLive " + idLive + " path: " + videoSource);
                    errors = EnumErrors.FILE_UPLOAD_ERROR;
                }
            }

        } catch (Exception e) {
            log.error("Debate error", e);
        } finally {
            Files.deleteIfExists(Paths.get("/opt/dump/" + stream + ".flv"));
            Files.deleteIfExists(Paths.get("/opt/dump/" + idLive + ".jpg"));
        }

        log.info("End result: {}", errors);

    }


}
