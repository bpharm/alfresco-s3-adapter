package local.pharm.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentStreamListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;

public class S3StreamListener implements ContentStreamListener {

    private static final Log logger = LogFactory.getLog(S3StreamListener.class);

    private S3ContentWriter writer;

    public S3StreamListener(S3ContentWriter writer) {

        this.writer = writer;

    }

    @Override
    public void contentStreamClosed() throws ContentIOException {

        File file = writer.getTempFile();
        long size = file.length();
        writer.setSize(size);
        try {
            logger.debug("Writing to hotbox s3://" + writer.getBucketName() + "/" + writer.getKey());
            AmazonS3 client = writer.getHbClient();
            client.putObject(new PutObjectRequest(writer.getBucketName(), writer.getKey(), writer.getTempFile()));
        } catch (Exception e) {
            logger.error("S3StreamListener Failed to Hotbox Upload File", e);
        }

    }
}
