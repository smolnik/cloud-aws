package net.adamsmolnik.provider.aws;

import java.io.InputStream;
import javax.inject.Inject;
import javax.inject.Singleton;
import net.adamsmolnik.entity.EntityReference;
import net.adamsmolnik.provider.EntityProvider;
import net.adamsmolnik.provider.EntitySaver;

@Singleton
public class S3EnitiySaver implements EntitySaver {

    @Inject
    private EntityProvider entityProvider;

    @Override
    public void save(EntityReference entityReference, long size, InputStream is) {
        entityProvider.persist(entityReference, size, is);
    }

}
