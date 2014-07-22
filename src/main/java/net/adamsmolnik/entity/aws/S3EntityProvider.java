package net.adamsmolnik.entity.aws;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import net.adamsmolnik.entity.FileEntity;
import net.adamsmolnik.provider.EntityProvider;
import net.adamsmolnik.util.Configuration;
import net.adamsmolnik.util.ConfigurationKeys;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

/**
 * @author ASmolnik
 *
 */
@Singleton
public class S3EntityProvider implements EntityProvider {

    @Inject
    private Configuration conf;

    private AmazonS3Client s3Client;

    @PostConstruct
    private void init() {
        s3Client = new AmazonS3Client(new BasicAWSCredentials(conf.getGlobalValue(ConfigurationKeys.ACCESS_KEY_ID.getKey()),
                conf.getGlobalValue(ConfigurationKeys.SECRET_KEY.getKey())));
    }

    @Override
    public FileEntity getFileEntity(String objectKey) {
        S3Object s3Object = s3Client.getObject(conf.getGlobalValue(ConfigurationKeys.BUCKET_NAME.getKey()), objectKey);
        Map<String, String> metadataMap = new HashMap<>();
        ObjectMetadata s3ObjectMetadata = s3Object.getObjectMetadata();
        metadataMap.put("contentLength", String.valueOf(s3ObjectMetadata.getContentLength()));
        return new FileEntity(s3Object.getObjectContent(), metadataMap);
    }

}
