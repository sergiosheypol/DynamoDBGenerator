import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.User;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class DynamoDBPopulator {

    final static String USER_MGMT_NAME = "USERMGMT";
    final static String PK = "dni";

    private AmazonDynamoDB dynamoDB;

    public DynamoDBPopulator() {
        dynamoDB = AmazonDynamoDBClientBuilder.standard().build();
    }

    public void populateDB() throws Exception{

        List<User> userList = new ArrayList<>();

        ObjectMapper objectMapper = new ObjectMapper();
        InputStream file = DynamoDBPopulator.class.getResourceAsStream("/generated.json");
        JsonNode jsonNode = objectMapper.readTree(file);
        jsonNode.elements().forEachRemaining(userJson -> {
            try {
                userList.add(objectMapper.treeToValue(userJson, User.class));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });

        File dniFile = new File(getClass().getClassLoader().getResource("dni.txt").getFile());
        Scanner sc = new Scanner(dniFile);

        List<String> dnis = new ArrayList<>();

        while (sc.hasNextLine()){
            dnis.add(sc.nextLine());
        }


        for (int i = 0; i< userList.size(); i++) {

            User user = userList.get(i);

            Map<String, AttributeValue> item = new HashMap<>();

            item.put(PK, new AttributeValue(dnis.get(i)));
//            item.put(SK, new AttributeValue(dnis.get(i)));
            item.put("age", new AttributeValue().withN("" + user.getAge()));
            item.put("name", new AttributeValue(user.getName()));
            item.put("registryDate", new AttributeValue(user.getRegistryDate()));
            item.put("gender", new AttributeValue(user.getGender()));
            item.put("email", new AttributeValue(user.getEmail()));
            item.put("phone", new AttributeValue(user.getPhone()));
            item.put("bookings", new AttributeValue().withM(new HashMap<>()));

            dynamoDB.putItem(USER_MGMT_NAME,item);
        }


    }


    public static void main(String[] args) {
        DynamoDBPopulator populator = new DynamoDBPopulator();
        try {
            populator.populateDB();
        } catch (Exception e) {
            System.err.println(e);
        }


    }
}
