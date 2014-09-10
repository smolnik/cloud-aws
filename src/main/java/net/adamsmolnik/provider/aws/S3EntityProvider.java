package net.adamsmolnik.provider.aws;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import net.adamsmolnik.entity.Entity;
import net.adamsmolnik.entity.EntityReference;
import net.adamsmolnik.entity.EntityReferenceDest;
import net.adamsmolnik.entity.EntityReferenceSource;
import net.adamsmolnik.entity.OperationDetails;
import net.adamsmolnik.provider.EntityProvider;
import net.adamsmolnik.util.Configuration;
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

    private AmazonS3Client s3;

    private String bucketName;

    @PostConstruct
    private void init() {
        s3 = new AmazonS3Client();
        bucketName = conf.getGlobalValue("bucketName");
    }

    @Override
    public Entity getEntity(EntityReference er) {
        S3Object s3Object = s3.getObject(bucketName, er.getEntityReferenceKey());
        return new Entity(s3Object.getObjectContent(), doGetMetadata(s3Object.getObjectMetadata(), er));
    }

    @Override
    public OperationDetails copy(EntityReferenceSource ers, EntityReferenceDest erd) {
        CopyObjectResult response = doCopy(ers, erd);
        return new OperationDetails(erd, response.getVersionId(), response.getETag());
    }

    private CopyObjectResult doCopy(EntityReferenceSource ers, EntityReferenceDest erd) {
        CopyObjectResult response = s3.copyObject(bucketName, ers.getEntityReferenceKey(), bucketName, erd.getEntityReferenceKey());
        return response;
    }

    @Override
    public void persist(EntityReference er, long size, InputStream is) {
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(size);
        s3.putObject(bucketName, er.getEntityReferenceKey(), is, om);
    }

    @Override
    public void delete(EntityReference er) {
        s3.deleteObject(bucketName, er.getEntityReferenceKey());

    }

    @Override
    public void setNewMetadata(EntityReference er, Map<String, String> metadata) {
        String objectKey = er.getEntityReferenceKey();
        CopyObjectRequest cor = new CopyObjectRequest(bucketName, objectKey, bucketName, objectKey);
        ObjectMetadata omd = s3.getObjectMetadata(bucketName, er.getEntityReferenceKey());
        omd.setUserMetadata(metadata);
        cor.setNewObjectMetadata(omd);
        s3.copyObject(cor);
    }

    private Map<String, String> doGetMetadata(ObjectMetadata omd, EntityReference er) {
        Map<String, String> metadataMap = new HashMap<>();
        metadataMap.put("contentLength", String.valueOf(omd.getContentLength()));
        metadataMap.putAll(omd.getUserMetadata());
        return metadataMap;
    }

    private Map<String, String> doGetMetadata(EntityReference er) {
        ObjectMetadata omd = s3.getObjectMetadata(bucketName, er.getEntityReferenceKey());
        return doGetMetadata(omd, er);
    }

    @Override
    public Map<String, String> getMetadata(EntityReference er) {
        return doGetMetadata(er);
    }

}
