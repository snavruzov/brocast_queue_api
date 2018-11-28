package com.dgtz.api.queue.services;

import com.dgtz.api.beans.MediaInfo;
import com.dgtz.api.contents.LiveShelf;
import com.dgtz.api.enums.EnumErrors;
import com.dgtz.api.feature.AmazonS3Module;
import com.dgtz.api.queue.utils.FileUtil;
import com.dgtz.api.queue.utils.HLSUploadModule;
import com.dgtz.api.queue.utils.MediaSettings;
import com.dgtz.db.api.domain.MediaStatus;
import com.dgtz.db.api.enums.EnumSQLErrors;
import com.dgtz.mcache.api.factory.Constants;
import com.dgtz.mcache.api.factory.RMemoryAPI;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by sardor on 3/13/16.
 */
public final class LiveStreamDoneFactory {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(LiveStreamDoneFactory.class);
    private static Long durationLimit = 7200l;

    private String stream;
    private String app;

    private Long idUser = 0L;
    private Long idLive = 0L;
    private Long duration = 0L;
    private String videoSource;
    private String hlsSource;
    private Integer rotation;

    public LiveStreamDoneFactory(){}

    public LiveStreamDoneFactory(String stream, String app, Long idUser, Long idLive, String streamFull, Integer rotation) {
        this.stream = stream;
        this.app = app;
        this.idUser = idUser;
        this.idLive = idLive;
        this.videoSource = "/opt/dump/live/rec/" + stream + ".flv";
        this.hlsSource = "/opt/hls_fragments/hls_pro/" + stream + "/";
    }

    public void doneIt() throws Exception {
        EnumErrors errors = EnumErrors.NO_ERRORS;
        try {

            String idm = RMemoryAPI.getInstance().pullHashFromMemory(Constants.MEDIA_KEY + idLive, "id_media");
            if (idLive > 0 && idUser >= 0 && idm!=null) {

                EnumSQLErrors sqlError = new LiveShelf().doneLivePublish(idLive, 1, (short) 0);
                log.info("UPDATE LIVE PROCESS, DEL FROM LIST {}", sqlError);

                FileUtil.mountStorageDisk();

                RMemoryAPI.getInstance().
                        pushHashToMemory(Constants.MEDIA_KEY + idLive, "progress", new MediaStatus(1).toString());

                AmazonS3Module s3Module = new AmazonS3Module();
                String link = Constants.encryptAmazonURL(idUser, idLive, "_original", "origin", "");
                String hls_url = Constants.encryptAmazonURL(idUser, idLive, "", "hls", "");
                String mp4_url = Constants.encryptAmazonURL(idUser, idLive, "_hi.mp4", "v", "");

                boolean isRecordHLSExists = Files.exists(Paths.get(hlsSource + "index.m3u8"));

                if (isRecordHLSExists) {

                    HLSUploadModule.prepLocalHLS(idLive, hlsSource, stream);

                    String localVideoSrc = "/opt/dump/" + System.currentTimeMillis() + ".flv";

                    FileUtil.copyFile(videoSource, localVideoSrc);
                    MediaSettings settings = new MediaSettings();
                    MediaInfo mediaInfo = settings.videoInfo(hlsSource + "index.m3u8");
                    if (mediaInfo != null) {
                        duration = mediaInfo.getDuration();

                        log.info("Live duration {}", duration);
                        if (duration != 0 && duration>3) {
                            FileUtil.processStoryBoard(s3Module, idUser, idLive, localVideoSrc, duration);
                        } else {
                            throw new Exception("Low video duration");
                        }

                        settings.saveVideoProperties(idLive, mediaInfo, true);

                        log.info("GETTING THUMBNAIL...for {}", idLive);
                        List<String> thumbs = RMemoryAPI.getInstance()
                                .pullListElemFromMemory(Constants.MEDIA_KEY + "thumblist:jpg:" + idLive, 0, -1);
                        if (thumbs == null || thumbs.isEmpty()) {
                            LiveStreamThumbnailFactory thumbnailFactory = new LiveStreamThumbnailFactory(videoSource, idLive, idUser, rotation);
                            thumbnailFactory.doThumb();
                        } else {
                            int thmNum = thumbs.size() / 2;
                            String thmURL = thumbs.get(thmNum);

                            String link_jpg = Constants.encryptAmazonURL(idUser, idLive, "jpg", "thumb", "", true);
                            String link_webp = Constants.encryptAmazonURL(idUser, idLive, "webp", "thumb", "", true);

                            s3Module.copyImageFile(thmURL, link_jpg);
                            s3Module.copyImageFile(thmURL, link_webp);
                        }


                        hls_url = hls_url.substring(hls_url.lastIndexOf("/") + 1);
                        log.info("HLS LIVE URL: {}", hls_url);
                        FileUtil.copyFile(hlsSource + "index.m3u8", hlsSource + hls_url);
                        s3Module.uploadHLSFolder(idLive, idUser + "/hls_vod/" + idLive + "/", hlsSource);
                        Thread.sleep(1000);
                        s3Module.uploadOriginalFile(link, localVideoSrc, 0);
                        Thread.sleep(5000);
                        FileUtil.copyFile(videoSource, localVideoSrc);
                        errors = s3Module.uploadOriginalFile(mp4_url, localVideoSrc, 0);

                    } else {
                        errors = com.dgtz.api.enums.EnumErrors.ERROR_IN_COMPRESSING;
                    }

                } else {
                    log.info("File is not recorded IDUSER " + idUser + " idLive " + idLive + " path: " + videoSource);
                    errors = EnumErrors.FILE_UPLOAD_ERROR;
                }

                if (errors != EnumErrors.NO_ERRORS) {
                    RMemoryAPI.getInstance().
                            pushHashToMemory(Constants.MEDIA_KEY + idLive, "progress", new MediaStatus(2).toString());
                }
            }

        } catch (Exception e) {
            log.error("error while processing queue done", e);
            RMemoryAPI.getInstance().
                    pushHashToMemory(Constants.MEDIA_KEY + idLive, "progress", new MediaStatus(2).toString());
        }


        try {

            MediaStatus status = RMemoryAPI.getInstance().
                    pullHashFromMemory(Constants.MEDIA_KEY + idLive, "progress", MediaStatus.class);


            if (status != null) {
                int progress = status.getProgress();
                if(progress==1) {
                    progress = 0;
//                    log.info("SENDING TO COMPRESSER MEDIA: {}", idLive);
//                    HttpResponse<JsonNode> body =
//                            Unirest.get(Constants.ENCODING_URL + "compresser/algo/tools/transcode/start?idm=" + idLive + "&idu=" + idUser)
//                                    .header("Host", "media.api.asalam.com")
//                                    .asJson();
//                    log.info("GET RESPONSE LIVE DONE: {}, {}", body.getBody().toString(), body.getBody().getObject().get("error").toString());
                }
                EnumSQLErrors sqlError = new LiveShelf().doneLivePublish(idLive, progress, duration.shortValue());

            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Files.deleteIfExists(Paths.get("/opt/dump/" + stream + ".flv"));
            Files.deleteIfExists(Paths.get("/opt/dump/" + idLive + ".jpg"));
        }


    }

}
