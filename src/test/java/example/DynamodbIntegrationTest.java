package example;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.GetItemEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class DynamodbIntegrationTest {
    private static final int DYNAMODB_PORT = 8000;

    private String dynamodbTableName;
    private Region dynamodbRegion;
    private String customDynamodbEndpoint;
    private DynamoDbClient ddb;

    @Container
    public GenericContainer dynamodb =
            new GenericContainer<>("amazon/dynamodb-local")
                    .withExposedPorts(DYNAMODB_PORT);

    @BeforeEach
    public void setUp() {
        String address = dynamodb.getHost();
        System.out.println("ddb address " + address);

        Integer port = dynamodb.getFirstMappedPort();
        System.out.println("ddb port " + port);

        this.dynamodbTableName = "example";
        this.dynamodbRegion = Region.US_EAST_1;
        this.customDynamodbEndpoint = "http://" + address + ":" + port;

        this.ddb = DynamoDbClient.builder()
                .region(dynamodbRegion)
                .endpointOverride(URI.create(customDynamodbEndpoint))
                .build();

        //check if table exists. if not create it
        if (!tableExists(this.ddb, dynamodbTableName)) {
            createTable(ddb, dynamodbTableName, "key");
        }
    }

    @Test
    void quickTest() {
        IntStream.range(0, 6).forEach(i -> {
            Example example = new Example();
            String primaryKey = "primaryKey" + UUID.randomUUID();
            example.setKey(primaryKey);
            example.setData("Hello");

            saveItem(dynamodbTableName, example);

            Example actual = getItem(dynamodbTableName, primaryKey);

            assertEquals(example, actual);
        });
    }

    private Example getItem(String dynamodbTableName, String primaryKey) {
        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();

        Key key = Key.builder()
                .partitionValue(primaryKey)
                .build();

        DynamoDbTable<Example> table = enhancedClient.table(dynamodbTableName, TableSchema.fromBean(Example.class));
        // Get the item by using the key.
        Example example = table.getItem((GetItemEnhancedRequest.Builder requestBuilder) -> requestBuilder.key(key));
        return example;
    }

    private void createTable(DynamoDbClient ddb, String tableName, String primaryKey) {

        String key = primaryKey;

        DynamoDbWaiter dbWaiter = ddb.waiter();
        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(key)
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .keySchema(KeySchemaElement.builder()
                        .attributeName(key)
                        .keyType(KeyType.HASH)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(new Long(10))
                        .writeCapacityUnits(new Long(10))
                        .build())
                .tableName(tableName)
                .build();

        try {
            CreateTableResponse response = ddb.createTable(request);
            DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();

            // Wait until the Amazon DynamoDB table is created
            WaiterResponse<DescribeTableResponse> waiterResponse =  dbWaiter.waitUntilTableExists(tableRequest);
            waiterResponse.matched().response().ifPresent(System.out::println);

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            throw e;
        }
    }

    private void saveItem(String dynamodbTableName, Example example) {
        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();

        DynamoDbTable<Example> table = enhancedClient.table(dynamodbTableName, TableSchema.fromBean(Example.class));
        table.putItem(example);
    }

    private boolean tableExists(DynamoDbClient ddb,String tableName ) {

        DescribeTableRequest request = DescribeTableRequest.builder()
                .tableName(tableName)
                .build();
        try {
            TableDescription tableInfo =
                    ddb.describeTable(request).table();

            if (tableInfo != null) {

                ProvisionedThroughputDescription throughputInfo =
                        tableInfo.provisionedThroughput();
                List<AttributeDefinition> attributes =
                        tableInfo.attributeDefinitions();
            }
        } catch (DynamoDbException e) {
            return false;
        }
        return true;
    }

}