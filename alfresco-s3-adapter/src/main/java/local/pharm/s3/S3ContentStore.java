package local.pharm.s3;

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.alfresco.repo.content.AbstractContentStore;
import org.alfresco.repo.content.ContentStore;
import org.alfresco.repo.content.ContentStoreCreatedEvent;
import org.alfresco.repo.content.UnsupportedContentUrlException;
import org.alfresco.repo.content.filestore.FileContentStore;
import org.alfresco.service.cmr.repository.ContentReader;
import org.alfresco.service.cmr.repository.ContentWriter;
import org.alfresco.util.GUID;
import org.alfresco.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Map;


public class S3ContentStore extends AbstractContentStore
        implements ApplicationContextAware, ApplicationListener<ApplicationEvent> {

    private static final Log logger = LogFactory.getLog(S3ContentStore.class);
    private ApplicationContext applicationContext;

    private AmazonS3 s3Client;
    private AmazonS3 hbClient;

    private String accessKey;
    private String secretKey;

    private String hbAccessKey;
    private String hbSecretKey;

    private String bucketName;
    private String regionName;
    private String rootDirectory;

    @Override
    public boolean isWriteSupported() {
        return true;
    }

    @Override
    public ContentReader getReader(String contentUrl) {

        String key = makeS3Key(contentUrl);
        return new S3ContentReader(key, contentUrl, hbClient, bucketName);

    }

    public void init() {
        try {
            if(StringUtils.isNotBlank(this.accessKey) && StringUtils.isNotBlank(this.secretKey)) {
                logger.debug("Found credentials in properties file");
                BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.hbAccessKey, this.hbSecretKey);
                /*
                s3Client = AmazonS3ClientBuilder.standard()
                        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                        .withRegion(this.regionName)
                        .build();
                */
                hbClient = AmazonS3ClientBuilder.standard()
                        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("hb.bizmrg.com", this.regionName))
                        .build();
            }
            else {
                logger.debug("AWS Credentials not specified in properties, will fallback to credentials provider");
                hbClient = AmazonS3ClientBuilder.standard()
                        .withCredentials(new EnvironmentVariableCredentialsProvider())
                        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("hb.bizmrg.com", this.regionName))
                        .build();
                /*
                s3Client = AmazonS3ClientBuilder.standard()
                        .withCredentials(new EnvironmentVariableCredentialsProvider())
                        .withRegion(this.regionName)
                        .build();
                */


            }
        } catch (Exception e) {
            logger.error("Can not find AWS Credentials", e);
        }
    }


    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public void setHbAccessKey(String hbAccessKey) {
        this.hbAccessKey = hbAccessKey;
    }

    public void setHbSecretKey(String hbSecretKey) {
        this.hbSecretKey = hbSecretKey;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public void setRootDirectory(String rootDirectory) {

        String dir = rootDirectory;
        if (dir.startsWith("/")) {
            dir = dir.substring(1);
        }

        this.rootDirectory = dir;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }


    @Override
    protected ContentWriter getWriterInternal(ContentReader existingContentReader, String newContentUrl) {

        String contentUrl = newContentUrl;

        if(StringUtils.isBlank(contentUrl)) {
            contentUrl = createNewUrl();
        }

        String key = makeS3Key(contentUrl);

        return new S3ContentWriter(bucketName, key, contentUrl, existingContentReader, hbClient);

    }

    public static String createNewUrl() {

        Calendar calendar = new GregorianCalendar();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;  // 0-based
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        // create the URL
        StringBuilder sb = new StringBuilder(20);
        sb.append(FileContentStore.STORE_PROTOCOL)
                .append(ContentStore.PROTOCOL_DELIMITER)
                .append(year).append('/')
                .append(month).append('/')
                .append(day).append('/')
                .append(hour).append('/')
                .append(minute).append('/')
                .append(GUID.generate()).append(".bin");
        String newContentUrl = sb.toString();
        // done
        return newContentUrl;

    }

    private String makeS3Key(String contentUrl)
    {
        // take just the part after the protocol
        Pair<String, String> urlParts = super.getContentUrlParts(contentUrl);
        String protocol = urlParts.getFirst();
        String relativePath = urlParts.getSecond();
        // Check the protocol
        if (!protocol.equals(FileContentStore.STORE_PROTOCOL))
        {
            throw new UnsupportedContentUrlException(this, protocol + PROTOCOL_DELIMITER + relativePath);
        }


        return rootDirectory + "/" + relativePath;

    }

    @Override
    public boolean delete(String contentUrl) {
        try {
            String key = makeS3Key(contentUrl);
            logger.debug("Deleting object from Hotbox S3 with url: " + contentUrl + ", key: " + key);
            hbClient.deleteObject(bucketName, key);
            return true;
        } catch (Exception e) {
            logger.error("Error deleting Hotbox S3 Object", e);
        }
        return false;
    }

    /**
     * Publishes an event to the application context that will notify any interested parties of the existence of this
     * content store.
     *
     * @param context
     *            the application context
     * @param extendedEventParams
     */
    private void publishEvent(ApplicationContext context, Map<String, Serializable> extendedEventParams)
    {
        context.publishEvent(new ContentStoreCreatedEvent(this, extendedEventParams));
    }

    public void onApplicationEvent(ApplicationEvent event)
    {
        // Once the context has been refreshed, we tell other interested beans about the existence of this content store
        // (e.g. for monitoring purposes)
        if (event instanceof ContextRefreshedEvent && event.getSource() == this.applicationContext)
        {
            publishEvent(((ContextRefreshedEvent) event).getApplicationContext(), Collections.<String, Serializable> emptyMap());
        }
    }
}
