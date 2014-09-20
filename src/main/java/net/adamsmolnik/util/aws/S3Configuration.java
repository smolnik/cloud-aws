package net.adamsmolnik.util.aws;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.PostConstruct;
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

    private static final String KEY_DELIMITER = "/";

    private static final String DOT = Pattern.quote(".");

    private final Map<String, String> localConfMap = new HashMap<>();

    private final Map<String, String> globalConfMap = new HashMap<>();

    private final Map<String, Map<String, String>> servicesConfMap = new HashMap<>();

    private String serviceName;

    @PostConstruct
    private void init() {
        Path hostConfFile = Paths.get("/home/ec2-user/host/conf.properties");
        Map<String, String> hostConfMap = new HashMap<>();

        if (Files.exists(hostConfFile) && Files.isRegularFile(hostConfFile)) {
            try {
                fillConfMap(Files.newInputStream(hostConfFile), hostConfMap);
            } catch (IOException e) {
                throw new ServiceException(e);
            }
        }
        InputStream bootstrapInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("bootstrap.properties");
        fillConfMap(bootstrapInputStream, localConfMap);
        // host-wide envName parameter wins and overrides the service level one
        String hostEnvName = hostConfMap.get("environmentName");
        String localEnvName = localConfMap.get("environmentName");
        String envName = hostEnvName != null && !hostEnvName.trim().isEmpty() ? hostEnvName : localEnvName;

        serviceName = localConfMap.get("serviceName");
        AmazonS3Client s3Client = new AmazonS3Client();
        S3Object s3Object = s3Client.getObject(envName, "conf/global.properties");
        fillConfMap(s3Object.getObjectContent(), globalConfMap);
        String bucketName = globalConfMap.get("bucketName");
        ObjectListing s3Objects = s3Client.listObjects(bucketName, "conf/services");
        List<S3ObjectSummary> summaries = s3Objects.getObjectSummaries();;
        for (S3ObjectSummary s3ObjectSummary : summaries) {
            String s3ObjectKey = s3ObjectSummary.getKey();
            String[] ss = s3ObjectKey.split(KEY_DELIMITER);
            String serviceKey = ss[ss.length - 1].split(DOT)[0];
            S3Object s3ObjectForService = s3Client.getObject(envName, s3ObjectKey);
            if (s3ObjectForService.getObjectMetadata().getContentLength() > 0) {
                Map<String, String> serviceConfMap = new HashMap<>();
                fillConfMap(s3ObjectForService.getObjectContent(), serviceConfMap);
                servicesConfMap.put(serviceKey, Collections.unmodifiableMap(serviceConfMap));
            }
        }
    }

    private static void fillConfMap(InputStream is, Map<String, String> confMap) {
        String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            while ((line = br.readLine()) != null) {
                String[] ss = line.split("=");
                confMap.put(ss[0], ss[1]);
            }
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    public String getServiceValue(String key) {
        Map<String, String> serviceConfMap = servicesConfMap.get(serviceName);
        return serviceConfMap == null ? null : serviceConfMap.get(key);
    }

    @Override
    public String getGlobalValue(String key) {
        return globalConfMap.get(key);
    }

    @Override
    public String getLocalValue(String key) {
        return localConfMap.get(key);
    }

    @Override
    public Map<String, String> getServiceConfMap() {
        return servicesConfMap.get(serviceName);
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public String toString() {
        return "S3Configuration [localConfMap=" + localConfMap + ", globalConfMap=" + globalConfMap + ", servicesConfMap=" + servicesConfMap
                + ", serviceName=" + serviceName + "]";
    }

}
