package connectors.test;

import connectors.RabbitMQConnection;
import lombok.extern.slf4j.Slf4j;
import model.QueueInformation;
import model.RabbitDomain;
import org.junit.jupiter.api.Test;

@Slf4j
class RabbitMQConnectionTest extends TestOnContainersCase {

    private final String[] connections = new String[] {"[CON1;localhost;"+container.getAmqpPort()+";guest;guest]", "[CON2;localhost;"+container.getAmqpPort()+";guest;guest]"};

    @Test
    void rabbitMqConnectionAllTests() {
        try {
            RabbitMQConnection rabbitMQConnection = RabbitMQConnection.builder().connections(this.connections).build();
            rabbitMQConnection.buildConnectionFactory();

            RabbitDomain rabbitDomain = rabbitMQConnection.rabbitDomain("CON1");
            rabbitDomain = rabbitMQConnection.createDirectExchangeForQueue("DOCUMENTS", "DIRECT-DOCUMENTS", "dd-rk", "CON1");
            rabbitDomain = rabbitMQConnection.createDirectExchangeForQueue("DOCUMENTS", "DIRECT-DOCUMENTS", "dd-rk", "CON1");
            rabbitMQConnection.closeConnection(rabbitDomain);

            rabbitDomain = rabbitMQConnection.rabbitDomain("CON2");
            rabbitDomain = rabbitMQConnection.createDirectExchangeForQueue("DOCUMENTS_MINING", "DIRECT-DOCUMENT-MINING", "ddm-rk", "CON2");
            rabbitMQConnection.closeConnection(rabbitDomain);
            
        }catch(Exception ex) {
            log.error(ex.getMessage());
        }
    }

    @Test
    void checkQueueInformation() throws Exception{
        RabbitMQConnection rabbitMQConnection = RabbitMQConnection.builder().connections(this.connections).build();
        rabbitMQConnection.buildConnectionFactory();
        try {
            QueueInformation queueInformationCON1 = rabbitMQConnection.queueExists("CON1", "DOCUMENTS");
            log.info(queueInformationCON1.toString());
        }catch(Exception ex) {
            log.info("DOCUMENTS nao existe no host %s, [error] = %s".formatted("CON1", ex.getMessage()));
        }

        try {
            QueueInformation queueInformationOCIPRD2 = rabbitMQConnection.queueExists("OCI-PRD", "DOCUMENTS_MINING");
            log.info(queueInformationOCIPRD2.toString());
        }catch(Exception ex) {
            log.info("DOCUMENTS nao existe no host %s, [error] = %s".formatted("CON2", ex.getMessage()));
        }
    }


    @Test
    void createDirectExchangeForQueue() {
        try {
            RabbitMQConnection rabbitMQConnection = RabbitMQConnection.builder().connections(this.connections).build();
            rabbitMQConnection.buildConnectionFactory();
            rabbitMQConnection.createDirectExchangeForQueue("DOCUMENTS", "DIRECT-DOCUMENTS", "dd-rk", "CON1");
        }catch(Exception ex) {
            log.error(ex.getMessage());
        }
    }

    @Test
    void createDirectDelayedExchangeForQueue() {
        try {
            RabbitMQConnection rabbitMQConnection = RabbitMQConnection.builder().connections(this.connections).build();
            rabbitMQConnection.buildConnectionFactory();
            rabbitMQConnection.createDirectExchangeDelayedMessageForQueue("QUEUE-DELAYED", "EXCHANGE-QUEUE-DELAYED", "exchange-queue-delayed-rk", "CON1");
        }catch(Exception ex) {
            log.error(ex.getMessage());
        }
    }

    @Test
    void sendAllTypeMessages() {
        sendMessagesTextPlain();
        sendMessagesObject();
        sendMessagesNumber();
    }

    void sendMessagesTextPlain() {
        try {
            RabbitMQConnection rabbitMQConnection = RabbitMQConnection.builder().connections(this.connections).build();
            rabbitMQConnection.buildConnectionFactory();
            rabbitMQConnection.createDirectExchangeForQueue("DOCUMENTS", "DIRECT-DOCUMENTS", "dd-rk", "CON1");
            rabbitMQConnection.sendTextMessage("CON1", "DIRECT-DOCUMENTS", "dd-rk", "Teste1");
            rabbitMQConnection.sendTextMessage("CON1", "DIRECT-DOCUMENTS", "dd-rk", "Teste2");
            rabbitMQConnection.sendTextMessage("CON1", "DIRECT-DOCUMENTS", "dd-rk", "Teste3");


            QueueInformation queueInformation = rabbitMQConnection.queueExists("CON1", "DOCUMENTS");
            System.out.println(queueInformation.toString());
        }catch(Exception ex) {
            log.error(ex.getMessage());
        }
    }
    void sendMessagesObject() {
        try {
            RabbitDomain rabbitDomain = RabbitDomain.builder().connectionId("CON1").build();
            RabbitMQConnection rabbitMQConnection = RabbitMQConnection.builder().connections(this.connections).build();
            rabbitMQConnection.buildConnectionFactory();
            rabbitMQConnection.createDirectExchangeForQueue("DOCUMENTS", "DIRECT-DOCUMENTS", "dd-rk", "CON1");
            rabbitMQConnection.sendObjectMessage("CON1", "DIRECT-DOCUMENTS", "dd-rk", rabbitDomain);

            QueueInformation queueInformation = rabbitMQConnection.queueExists("CON1", "DOCUMENTS");
            System.out.println(queueInformation.toString());
        }catch(Exception ex) {
            log.error(ex.getMessage());
        }
    }
    void sendMessagesNumber() {
        try {
            RabbitMQConnection rabbitMQConnection = RabbitMQConnection.builder().connections(this.connections).build();
            rabbitMQConnection.buildConnectionFactory();
            rabbitMQConnection.createDirectExchangeForQueue("DOCUMENTS", "DOCUMENTS-DOCUMENTS", "dd-rk", "CON1");
            Long value = Long.valueOf(1);
            rabbitMQConnection.sendObjectMessage("CON1", "DOCUMENTS-DOCUMENTS", "dd-rk", value);

            QueueInformation queueInformation = rabbitMQConnection.queueExists("CON1", "DOCUMENTS");
            System.out.println(queueInformation.toString());
        }catch(Exception ex) {
            log.error(ex.getMessage());
        }
    }

    @Test
    void checkInformationAllQueues() {
        try {
            RabbitMQConnection rabbitMQConnection = RabbitMQConnection.builder().connections(this.connections).build();
            rabbitMQConnection.buildConnectionFactory();

            QueueInformation queueInformation = rabbitMQConnection.queueExists("CON1", "DOCUMENTS");
            System.out.println(queueInformation.toString());

            queueInformation = rabbitMQConnection.queueExists("CON1", "DOCUMENTS");
            System.out.println(queueInformation.toString());

            rabbitMQConnection.queueExists("CON1", "DOCUMENTS");
            System.out.println(queueInformation.toString());
        }catch(Exception ex) {
            log.error(ex.getMessage());
        }
    }

    @Test
    void dropQueue() {
        try {
            RabbitMQConnection rabbitMQConnection = RabbitMQConnection.builder().connections(this.connections).build();
            rabbitMQConnection.buildConnectionFactory();

            int messages = rabbitMQConnection.dropQueue("DOCUMENTS", "CON1");
            log.info("Queue dropped, messages has been erased => "+messages);
        }catch(Exception ex) {
            log.error(ex.getMessage());
        }
    }
}

