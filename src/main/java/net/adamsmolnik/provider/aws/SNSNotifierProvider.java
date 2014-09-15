package net.adamsmolnik.provider.aws;

import java.util.Map;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import net.adamsmolnik.provider.NotifierProvider;
import net.adamsmolnik.util.Configuration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;

@Singleton
public class SNSNotifierProvider implements NotifierProvider {

    @Inject
    private Configuration conf;

    private AmazonSNSClient snsClient;

    private String topicArn;

    @PostConstruct
    public void init() {
        Map<String, String> confMap = conf.getServiceConfMap();
        snsClient = new AmazonSNSClient();
        snsClient.setRegion(Region.getRegion(Regions.US_EAST_1));
        CreateTopicRequest createTopicRequest = new CreateTopicRequest(confMap.get("sns.topic"));
        CreateTopicResult createTopicResult = snsClient.createTopic(createTopicRequest);
        topicArn = createTopicResult.getTopicArn();
    }

    @PreDestroy
    void destroy() {
        DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest(topicArn);
        snsClient.deleteTopic(deleteTopicRequest);
    }

    @Override
    public void subscribe(String email) {
        SubscribeRequest subRequest = new SubscribeRequest(topicArn, "email", email);
        snsClient.subscribe(subRequest);
    }

    @Override
    public void publish(String message) {
        PublishRequest publishRequest = new PublishRequest(topicArn, message);
        snsClient.publish(publishRequest);
    }

}
