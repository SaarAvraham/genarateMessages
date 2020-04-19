package com.example.demo;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import com.microsoft.sqlserver.jdbc.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.core.AbstractDestinationResolvingMessagingTemplate;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
public class DemoApplication {

    public static ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setPort(5672);
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
//        connectionFactory.setRequestedHeartBeat(20);
        return connectionFactory;
    }

    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        //The routing key is set to the name of the queue by the broker for the default exchange.
//        template.setRoutingKey(rabbitmqQueueName);
        template.setExchange("tut.fanout");
        //Where we will synchronously receive messages from
//        template.setDefaultReceiveQueue(rabbitmqQueueName);
        return template;
    }

    static RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory());


    public static void main(String[] args) throws IOException, TimeoutException, SQLException, InterruptedException {
//		SpringApplication.run(DemoApplication.class, args);
        generatedMessages();
//        populateDataLake();
        Thread.sleep(40000);
        System.exit(0);
    }

    public static void generatedMessages() throws IOException, TimeoutException {
        for (int i = 0; i < 5000000; i++) {
            String message = String.valueOf(i);
            rabbitTemplate.convertAndSend("tut.fanout", "", message);
        }


    }

    public static void populateDataLake() throws SQLException {
        for (int i = 0; i < 50; i++) {
            Connection conn = DriverManager.getConnection("jdbc:sqlserver://DESKTOP-S42L7FK\\MSSQLSERVER01;databaseName=Data-Lake;", "sa", "sa"); // TODO: 4/14/2020 Use Connection pool/SPRING JDBC
            PreparedStatement ps = conn.prepareStatement("INSERT INTO dt (value) VALUES (?) ");
            Instant start = Instant.now();
            for (int j = 0; j < 20000; j++) {
                ps.setString(1, String.format("%s %d", json, j));
                ps.addBatch();
                ps.clearParameters();
            }

            int[] insertions = ps.executeBatch();
            Instant end = Instant.now();
            System.out.println("Time taken: " + Duration.between(start, end).getSeconds() + " seconds");
            conn.close();
        }
    }

//    public static void bcp() throws SQLException {
//        Connection conn = DriverManager.getConnection("jdbc:sqlserver://DESKTOP-S42L7FK\\MSSQLSERVER01;databaseName=Data-Lake;", "sa", "sa"); // TODO: 4/14/2020 Use Connection pool/SPRING JDBC
//
//        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn);
//        bulkCopy.setBulkCopyOptions(new SQLServerBulkCopyOptions().);
//
//        for(int i=0; i<3000; i++){
//
//        }
//    }

    private static final String json = "{\"web-app\": {\n" +
            "  \"servlet\": [   \n" +
            "    {\n" +
            "      \"servlet-name\": \"cofaxCDS\",\n" +
            "      \"servlet-class\": \"org.cofax.cds.CDSServlet\",\n" +
            "      \"init-param\": {\n" +
            "        \"configGlossary:installationAt\": \"Philadelphia, PA\",\n" +
            "        \"configGlossary:adminEmail\": \"ksm@pobox.com\",\n" +
            "        \"configGlossary:poweredBy\": \"Cofax\",\n" +
            "        \"configGlossary:poweredByIcon\": \"/images/cofax.gif\",\n" +
            "        \"configGlossary:staticPath\": \"/content/static\",\n" +
            "        \"templateProcessorClass\": \"org.cofax.WysiwygTemplate\",\n" +
            "        \"templateLoaderClass\": \"org.cofax.FilesTemplateLoader\",\n" +
            "        \"templatePath\": \"templates\",\n" +
            "        \"templateOverridePath\": \"\",\n" +
            "        \"defaultListTemplate\": \"listTemplate.htm\",\n" +
            "        \"defaultFileTemplate\": \"articleTemplate.htm\",\n" +
            "        \"useJSP\": false,\n" +
            "        \"jspListTemplate\": \"listTemplate.jsp\",\n" +
            "        \"jspFileTemplate\": \"articleTemplate.jsp\",\n" +
            "        \"cachePackageTagsTrack\": 200,\n" +
            "        \"cachePackageTagsStore\": 200,\n" +
            "        \"cachePackageTagsRefresh\": 60,\n" +
            "        \"cacheTemplatesTrack\": 100,\n" +
            "        \"cacheTemplatesStore\": 50,\n" +
            "        \"cacheTemplatesRefresh\": 15,\n" +
            "        \"cachePagesTrack\": 200,\n" +
            "        \"cachePagesStore\": 100,\n" +
            "        \"cachePagesRefresh\": 10,\n" +
            "        \"cachePagesDirtyRead\": 10,\n" +
            "        \"searchEngineListTemplate\": \"forSearchEnginesList.htm\",\n" +
            "        \"searchEngineFileTemplate\": \"forSearchEngines.htm\",\n" +
            "        \"searchEngineRobotsDb\": \"WEB-INF/robots.db\",\n" +
            "        \"useDataStore\": true,\n" +
            "        \"dataStoreClass\": \"org.cofax.SqlDataStore\",\n" +
            "        \"redirectionClass\": \"org.cofax.SqlRedirection\",\n" +
            "        \"dataStoreName\": \"cofax\",\n" +
            "        \"dataStoreDriver\": \"com.microsoft.jdbc.sqlserver.SQLServerDriver\",\n" +
            "        \"dataStoreUrl\": \"jdbc:microsoft:sqlserver://LOCALHOST:1433;DatabaseName=goon\",\n" +
            "        \"dataStoreUser\": \"sa\",\n" +
            "        \"dataStorePassword\": \"dataStoreTestQuery\",\n" +
            "        \"dataStoreTestQuery\": \"SET NOCOUNT ON;select test='test';\",\n" +
            "        \"dataStoreLogFile\": \"/usr/local/tomcat/logs/datastore.log\",\n" +
            "        \"dataStoreInitConns\": 10,\n" +
            "        \"dataStoreMaxConns\": 100,\n" +
            "        \"dataStoreConnUsageLimit\": 100,\n" +
            "        \"dataStoreLogLevel\": \"debug\",\n" +
            "        \"maxUrlLength\": 500}},\n" +
            "    {\n" +
            "      \"servlet-name\": \"cofaxEmail\",\n" +
            "      \"servlet-class\": \"org.cofax.cds.EmailServlet\",\n" +
            "      \"init-param\": {\n" +
            "      \"mailHost\": \"mail1\",\n" +
            "      \"mailHostOverride\": \"mail2\"}},\n" +
            "    {\n" +
            "      \"servlet-name\": \"cofaxAdmin\",\n" +
            "      \"servlet-class\": \"org.cofax.cds.AdminServlet\"},\n" +
            " \n" +
            "    {\n" +
            "      \"servlet-name\": \"fileServlet\",\n" +
            "      \"servlet-class\": \"org.cofax.cds.FileServlet\"},\n" +
            "    {\n" +
            "      \"servlet-name\": \"cofaxTools\",\n" +
            "      \"servlet-class\": \"org.cofax.cms.CofaxToolsServlet\",\n" +
            "      \"init-param\": {\n" +
            "        \"templatePath\": \"toolstemplates/\",\n" +
            "        \"log\": 1,\n" +
            "        \"logLocation\": \"/usr/local/tomcat/logs/CofaxTools.log\",\n" +
            "        \"logMaxSize\": \"\",\n" +
            "        \"dataLog\": 1,\n" +
            "        \"dataLogLocation\": \"/usr/local/tomcat/logs/dataLog.log\",\n" +
            "        \"dataLogMaxSize\": \"\",\n" +
            "        \"removePageCache\": \"/content/admin/remove?cache=pages&id=\",\n" +
            "        \"removeTemplateCache\": \"/content/admin/remove?cache=templates&id=\",\n" +
            "        \"fileTransferFolder\": \"/usr/local/tomcat/webapps/content/fileTransferFolder\",\n" +
            "        \"lookInContext\": 1,\n" +
            "        \"adminGroupID\": 4,\n" +
            "        \"betaServer\": true}}],\n" +
            "  \"servlet-mapping\": {\n" +
            "    \"cofaxCDS\": \"/\",\n" +
            "    \"cofaxEmail\": \"/cofaxutil/aemail/*\",\n" +
            "    \"cofaxAdmin\": \"/admin/*\",\n" +
            "    \"fileServlet\": \"/static/*\",\n" +
            "    \"cofaxTools\": \"/tools/*\"},\n" +
            " \n" +
            "  \"taglib\": {\n" +
            "    \"taglib-uri\": \"cofax.tld\",\n" +
            "    \"taglib-location\": \"/WEB-INF/tlds/cofax.tld\"}}}";
}
