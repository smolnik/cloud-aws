package net.adamsmolnik.util.aws;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.inject.Singleton;
import net.adamsmolnik.exceptions.ServiceException;
import net.adamsmolnik.util.Configuration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * @author ASmolnik
 *
 */
@Singleton
public class S3Configuration implements Configuration {

    private static final String BUCKET_NAME = "net.adamsmolnik.warsjawa";

    private static final String KEY_DELIMITER = "/";

    private static final String DOT = Pattern.quote(".");

    private Map<String, String> globalConfMap = new HashMap<>();

    private Map<String, Map<String, String>> servicesConfMap = new HashMap<>();

    public S3Configuration() {
        final AmazonS3Client s3Client = new AmazonS3Client();
        S3Object s3Object = s3Client.getObject(BUCKET_NAME, "conf/global.properties");
        fillConfMap(s3Object, globalConfMap);
        ObjectListing s3Objects = s3Client.listObjects(BUCKET_NAME, "conf/services");
        List<S3ObjectSummary> summaries = s3Objects.getObjectSummaries();;
        for (S3ObjectSummary s3ObjectSummary : summaries) {
            String s3ObjectKey = s3ObjectSummary.getKey();
            String[] ss = s3ObjectKey.split(KEY_DELIMITER);
            String serviceKey = ss[ss.length - 1].split(DOT)[0];
            S3Object s3ObjectForService = s3Client.getObject(BUCKET_NAME, s3ObjectKey);
            if (s3ObjectForService.getObjectMetadata().getContentLength() > 0) {
                Map<String, String> serviceConfMap = new HashMap<>();
                fillConfMap(s3ObjectForService, serviceConfMap);
                servicesConfMap.put(serviceKey, Collections.unmodifiableMap(serviceConfMap));
            }
        }
    }

    private static void fillConfMap(S3Object s3Object, Map<String, String> confMap) {
        String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8))) {
            while ((line = br.readLine()) != null) {
                String[] ss = line.split("=");
                confMap.put(ss[0], ss[1]);
            }
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    public String getServiceValue(String serviceName, String key) {
        Map<String, String> serviceConfMap = servicesConfMap.get(serviceName);
        return serviceConfMap == null ? null : serviceConfMap.get(key);
    }

    @Override
    public String getGlobalValue(String key) {
        return globalConfMap.get(key);
    }

    @Override
    public Map<String, String> getServiceConfMap(String serviceName) {
        return servicesConfMap.get(serviceName);
    }

}
