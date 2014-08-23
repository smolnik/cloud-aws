package net.adamsmolnik.provider.aws;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import net.adamsmolnik.entity.Entity;
import net.adamsmolnik.entity.EntityDetails;
import net.adamsmolnik.entity.EntityReference;
import net.adamsmolnik.entity.EntityReferenceDest;
import net.adamsmolnik.entity.EntityReferenceSource;
import net.adamsmolnik.provider.EntityProvider;
import net.adamsmolnik.util.Configuration;
import net.adamsmolnik.util.ConfigurationKeys;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
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

    private String bucketName;

    @PostConstruct
    private void init() {
        s3Client = conf.isSystemCredentialsExist() ? new AmazonS3Client(new BasicAWSCredentials(conf.getGlobalValue(ConfigurationKeys.ACCESS_KEY_ID
                .getKey()), conf.getGlobalValue(ConfigurationKeys.SECRET_KEY.getKey()))) : new AmazonS3Client();
        bucketName = conf.getGlobalValue(ConfigurationKeys.BUCKET_NAME.getKey());
    }

    @Override
    public Entity getEntity(EntityReference er) {
        S3Object s3Object = s3Client.getObject(bucketName, er.getEntityReferenceKey());
        Map<String, String> metadataMap = new HashMap<>();
        ObjectMetadata s3ObjectMetadata = s3Object.getObjectMetadata();
        metadataMap.put("contentLength", String.valueOf(s3ObjectMetadata.getContentLength()));
        return new Entity(s3Object.getObjectContent(), metadataMap);
    }

    @Override
    public EntityDetails copy(EntityReferenceSource ers, EntityReferenceDest erd) {
        CopyObjectResult response = doCopy(ers, erd);
        return new EntityDetails(erd, response.getVersionId(), response.getETag());
    }

    private CopyObjectResult doCopy(EntityReferenceSource ers, EntityReferenceDest erd) {
        CopyObjectResult response = s3Client.copyObject(bucketName, ers.getEntityReferenceKey(), bucketName, erd.getEntityReferenceKey());
        return response;
    }

    @Override
    public EntityDetails move(EntityReferenceSource ers, EntityReferenceDest erd) {
        CopyObjectResult response = doCopy(ers, erd);
        s3Client.deleteObject(bucketName, ers.getEntityReferenceKey());
        return new EntityDetails(erd, response.getVersionId(), response.getETag());
    }

    @Override
    public void persist(EntityReference entityReference, long size, InputStream is) {
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(size);
        s3Client.putObject(bucketName, entityReference.getEntityReferenceKey(), is, om);
    }

    @Override
    public void setNewMetadata(EntityReference entityReference, String key, String value) {
        String objectKey = entityReference.getEntityReferenceKey();
        CopyObjectRequest cor = new CopyObjectRequest(bucketName, objectKey, bucketName, objectKey);
        ObjectMetadata omd = new ObjectMetadata();
        omd.addUserMetadata(key, value);
        cor.setNewObjectMetadata(omd);
        s3Client.copyObject(cor);
    }

}
