package com.dgtz.api.queue.services;

import com.dgtz.api.feature.ThumbnailBuilder;
import com.dgtz.api.queue.utils.FileUtil;
import com.dgtz.api.queue.utils.MediaSettings;
import com.dgtz.mcache.api.factory.Constants;
import com.dgtz.mcache.api.factory.RMemoryAPI;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * BroCast.
 * Copyright: Sardor Navruzov
 * 2013-2016.
 */
public class LiveStreamThumbnailFactory {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(LiveStreamThumbnailFactory.class);
    private String path;
    private Long idLive;
    private Long idUser;
    private Integer rotation;

    public LiveStreamThumbnailFactory(String path, Long idLive, Long idUser, Integer rotation) {
        this.path = path;
        this.idLive = idLive;
        this.idUser = idUser;
        this.rotation = rotation;

    }

    public void doThumb() throws IOException {

        log.info("RECORDED PARAM name: {}", path);

        try {

            FileUtil.mountStorageDisk();
            if (Files.exists(Paths.get(path))) {

                String localPath = "/opt/dump/thm_"+System.currentTimeMillis()+".flv";
                FileUtil.copyFile(path, localPath);

                log.info("MOVING TO THUMB FOLDER {}", localPath);


                MediaSettings settings = new MediaSettings();
                ThumbnailBuilder thumbnailBuilder = new ThumbnailBuilder();
                boolean covered = thumbnailBuilder.thumbnailExtractor(localPath, "/opt/dump/" + idLive + "/", idLive, rotation);
                settings.s3ThumbnailUploader(idUser, idLive, !covered, true);

                String jpg = Constants.encryptAmazonURL(idUser, idLive, "jpg", "thumb", "");
                //String webp = Constants.encryptAmazonURL(idUser, idLive, "webp", "thumb", "");

                RMemoryAPI.getInstance().pushUnlimitedListToMemory(Constants.MEDIA_KEY + "thumblist:jpg:" + idLive, jpg);
                //RMemoryAPI.getInstance().pushUnlimitedListToMemory(Constants.MEDIA_KEY+"thumblist:webp:"+idLive, webp);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void thumbnailProducer(){
        MediaSettings settings = new MediaSettings();
        ThumbnailBuilder thumbnailBuilder = new ThumbnailBuilder();
        boolean covered = thumbnailBuilder.thumbnailExtractor(path, "/opt/dump/" + idLive + "/", idLive, 0);
        settings.s3ThumbnailUploader(idUser, idLive, !covered, true);
    }


}
