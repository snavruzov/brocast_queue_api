package com.dgtz.api.queue.utils;

import com.brocast.riak.api.beans.DcMediaEntity;
import com.dgtz.api.beans.MediaInfo;
import com.dgtz.api.beans.MediaJSObject;
import com.dgtz.api.beans.MediaStreamFormat;
import com.dgtz.api.contents.MediaShelf;
import com.dgtz.api.feature.AmazonS3Module;
import com.dgtz.mcache.api.factory.Constants;
import com.dgtz.mcache.api.factory.RMemoryAPI;
import com.google.gson.Gson;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Digital Citizen.
 * User: Sardor Navuzov
 * Date: 11/20/14
 */
public class MediaSettings {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MediaSettings.class);


    public MediaSettings() {
    }


    public MediaInfo videoInfo(String filename) {
        Gson gson = new Gson();
        MediaInfo mediaInfo = new MediaInfo();
        String js = FileUtil.getJsonInfoOfVideo(filename);
        log.info("JS SHELL {}", js);
        if (js == null || js.isEmpty()) {
            return null;
        }
        try {
            MediaJSObject jsObject = gson.fromJson(js, MediaJSObject.class);
            mediaInfo.setDuration((long) jsObject.getFormat().getDuration());
            mediaInfo.setSize((jsObject.getFormat().getSize() / 1024) / 1024);
            mediaInfo.setFilename(jsObject.getFormat().getFilename());
            for (MediaStreamFormat ms : jsObject.getStreams()) {
                if (ms.getCodec_type().equals("video")) {
                /*===============*/
                    long vb = 0;
                    if (ms.getBit_rate() == null) {
                        vb = jsObject.getFormat().getBit_rate();
                    } else {
                        vb = ms.getBit_rate();
                    }
                /*=======END========*/
                    mediaInfo.setVbitrate(vb / 1024);
                    mediaInfo.setHeight(ms.getHeight());
                    mediaInfo.setWidth(ms.getWidth());
                    mediaInfo.setType("video");

                    Double framerate = 23.98;
                    String fps = ms.getR_frame_rate();
                    if(fps!=null) {
                        String numer = fps.substring(0,fps.indexOf("/"));
                        String denom = fps.substring(fps.indexOf("/")+1,fps.length());
                        framerate = Double.valueOf(numer)/Double.valueOf(denom);
                    }
                    mediaInfo.setFps(framerate);

                    if (ms.getTags() != null && ms.getTags().getRotate() != null && ms.getTags().getRotate() == 90)
                        mediaInfo.setRotate(true);
                }

                if (ms.getCodec_type().equals("audio")) {
                /*===============*/
                    long ab = 0;
                    if (ms.getBit_rate() == null) {
                        ab = 131072;
                    } else {
                        ab = ms.getBit_rate();
                    }
                /*=======END========*/
                    mediaInfo.setAbitrate(ab / 1024);
                    mediaInfo.setChannels(ms.getChannels());
                }

            }


            log.info(mediaInfo.toString());
        } catch (Exception e) {
            log.error("ERROR IN THE MAIN API, GETTING FFPROBE INFO: ", e);
            mediaInfo = null;
        }
        return mediaInfo;
    }

    public void saveVideoProperties(Long idMedia, MediaInfo mediaInfo, boolean isliveANDDone) {
        if(isliveANDDone) {
            DcMediaEntity entity = new DcMediaEntity();
            entity.setDuration(mediaInfo.getDuration().intValue());
            entity.setIdMedia(idMedia);

            MediaShelf mediaShelf = new MediaShelf();
            mediaShelf.updateMediaTechProps(entity);
        }
        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "properties:" + idMedia, "duration", "" + mediaInfo.getDuration());
        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "properties:" + idMedia, "type", "media/v");
        //RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "properties:" + idMedia, "bitrate", mediaInfo.getVbitrate() + "");
        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "properties:" + idMedia, "path", mediaInfo.getFilename());
        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "properties:" + idMedia, "fps", mediaInfo.getFps()+"");


        String ratio = getAspectRatio(mediaInfo.getWidth(), mediaInfo.getHeight(), false);

        if (mediaInfo.isRotate()) {
            ratio = getAspectRatio(mediaInfo.getWidth(), mediaInfo.getHeight(), true);
        }

        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "properties:" + idMedia, "ratio", ratio);
        //RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "properties:" + idMedia, "abitrate", mediaInfo.getAbitrate() + "");
        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "properties:" + idMedia, "size", mediaInfo.getSize() + "");
        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "properties:" + idMedia, "width", mediaInfo.getWidth() + "");
        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "properties:" + idMedia, "height", mediaInfo.getHeight() + "");
    }

    public void saveLiveProperties(Long idMedia, MediaInfo mediaInfo) {

        String ratio = getAspectRatio(mediaInfo.getWidth(), mediaInfo.getHeight(), false);

        if (mediaInfo.isRotate()) {
            ratio = getAspectRatio(mediaInfo.getWidth(), mediaInfo.getHeight(), true);
        }

        RMemoryAPI.getInstance().pushHashToMemory(Constants.MEDIA_KEY + "properties:" + idMedia, "ratio", ratio);
    }

    public void s3ThumbnailUploader(long idUser, long idMedia, boolean isDefault, boolean refresh) {

        String old_jpg = "";
        String old_webp = "";
        try {
            AmazonS3Module s3Module = new AmazonS3Module();

            old_jpg = Constants.encryptAmazonURL(idUser, idMedia, "jpg", "thumb", "");
            old_webp = Constants.encryptAmazonURL(idUser, idMedia, "webp", "thumb", "");

            String link_jpg = Constants.encryptAmazonURL(idUser, idMedia, "jpg", "thumb", "", refresh);
            String link_webp = Constants.encryptAmazonURL(idUser, idMedia, "webp", "thumb", "", refresh);
            //String link_blur = Constants.encryptAmazonURL(idUser, idMedia, "blur", "thumb", "", refresh);

            if (isDefault) {
                s3Module.copyImageFile("defaults/media.jpg", link_jpg);
                s3Module.copyImageFile("defaults/media.webp", link_webp);
              //  s3Module.copyImageFile("defaults/blur.jpg", link_blur);
            } else {
                String localFileJPG = "/opt/dump/" + idMedia + "_" + System.currentTimeMillis() + ".jpg";
                //String localFileWEBP = "/opt/dump/" + idMedia + "_" + System.currentTimeMillis() + ".webp";
                //String localFileBLUR = "/opt/dump/" + idMedia + "_" + System.currentTimeMillis() + "_blur.jpg";

                FileUtil.copyFile("/opt/dump/" + idMedia + "/" + idMedia + ".jpg", localFileJPG);
                //FileUtil.copyFile("/opt/dump/" + idMedia + "/" + idMedia + ".webp", localFileWEBP);
                //FileUtil.processBlurThumbnail(s3Module, localFileJPG, localFileBLUR, link_blur);

                Files.deleteIfExists(Paths.get("/opt/dump/" + idMedia + "/00000001.jpg"));
                Files.deleteIfExists(Paths.get("/opt/dump/" + idMedia + "/" + idMedia + ".jpg"));
                Files.deleteIfExists(Paths.get("/opt/dump/" + idMedia + "/" + idMedia + ".webp"));

                log.info("Uploading to AWS {}", link_jpg);

                s3Module.uploadImageFile(link_jpg, localFileJPG);
                //s3Module.uploadOriginalFile(link_webp, localFileWEBP, 1);
                RMemoryAPI.getInstance().pushUnlimitedListToMemory(Constants.MEDIA_KEY + "thmrawlist:jpg:" + idMedia, localFileJPG);

            }

        } catch (Exception e) {
            rollBackThumbURL(idMedia, old_jpg, "jpg");
            rollBackThumbURL(idMedia, old_webp, "webp");
            e.printStackTrace();
        }

    }

    public void s3ThumbnailUploader(long idUser, long idMedia, boolean isDefault) {
        try {
            AmazonS3Module s3Module = new AmazonS3Module();
            String link_jpg = Constants.encryptAmazonURL(idUser, idMedia, "jpg", "thumb", "");
            String link_webp = Constants.encryptAmazonURL(idUser, idMedia, "webp", "thumb", "");

            if (isDefault) {
                s3Module.copyImageFile("defaults/media.jpg", link_jpg);
                s3Module.copyImageFile("defaults/media.webp", link_webp);
            } else {
                s3Module.uploadImageFile(link_jpg, "/opt/dump/" + idMedia + "/" + idMedia + ".jpg");
                //s3Module.uploadImageFile(link_webp, "/opt/dump/" + idMedia + "/" + idMedia + ".webp");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private String getAspectRatio(int w, int h, boolean rotate) {
        int rem;
        w = w == 0 ? 320 : w;
        h = h == 0 ? 240 : h;

        int newW = w;
        int newH = h;

        while (h != 0) {
            rem = w % h;
            w = h;
            h = rem;
        }

        newH = newH / w;
        newW = newW / w;

        String ratio = newW + ":" + newH;
        if (rotate) {
            ratio = newH + ":" + newW;
        }
        return ratio;
    }

    private void rollBackThumbURL(long idMedia, String url, String format) {
        RMemoryAPI.getInstance()
                .pushHashToMemory(Constants.MEDIA_KEY + "s3_link:" + "thumb" + idMedia + format, "url", url.trim());
    }
}
