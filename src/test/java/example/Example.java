package example;

import lombok.Data;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

import java.util.List;
import java.util.Map;

@Data
@DynamoDbBean
public class Example {

    private String key;

    private String data;

    @DynamoDbPartitionKey
    public String getKey() {
        return key;
    }

}