import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import java.util.ArrayList;
import java.util.List;

public class DynamoDBConfig {

    final static String ENDPOINT = "http://localhost:8000";
    final static String FLIGHT_MGMT_NAME = "FLIGHT-MGMT";
    final static String USER_MGMT_NAME = "USER-MGMT";

    private AmazonDynamoDB dynamoDB;

    public DynamoDBConfig() {
        dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(ENDPOINT, "us-west-2"))
                .build();
    }

    private void createTable(String name, String partitionKey, String sortKey) {

        boolean exists = dynamoDB.listTables().getTableNames().contains(name);

        if (exists) {
            System.out.println("Table " + name + " already exists");
            return;
        }

        // Common attributes for tables
        List<AttributeDefinition> attributes = new ArrayList<>();
        attributes.add(new AttributeDefinition(partitionKey, ScalarAttributeType.S));
        attributes.add(new AttributeDefinition(sortKey, ScalarAttributeType.S));

        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement(partitionKey, KeyType.HASH)); // HASH == partition Key
        keySchema.add(new KeySchemaElement(sortKey, KeyType.RANGE)); // RANGE == sort key

        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput(10L, 10L);

        dynamoDB.createTable(attributes, name, keySchema, provisionedThroughput);
    }

    public static void main(String[] args) {
        String flightMgmtPK = "id";
        String flightMgmtSK = "type";

        DynamoDBConfig config = new DynamoDBConfig();

        config.createTable(DynamoDBConfig.FLIGHT_MGMT_NAME, flightMgmtPK, flightMgmtSK);
        config.createTable(DynamoDBConfig.USER_MGMT_NAME, flightMgmtPK, flightMgmtSK);


    }
}
