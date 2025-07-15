package connectors;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import converters.TreatMessageContent;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import model.QueueInformation;
import model.RabbitDomain;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

@Builder
@Slf4j
/**
 * Working with RabbitMQ instances.
 * This class could be used to establish connection with RABBITMQ instances those were declared in another server machines
 * Could be used to set up all "Messenger" environment, for example, to define queues, exchanges and to create bindings among queues and exchanges
 * @author mateusgobo
 */
public class RabbitMQConnection {

    /**
     * Fill this attribute according this pattern = [connectionId1;host1;port1;user1;pass1],
     * [connectionId2;host2;port2;user2;pass2]...
     * [connectionIdN;hostN;portN;userN;passN]
     */
    private String[] connections;
    private ConnectionFactory connectionFactory = null;
    private List<RabbitDomain> rabbitConnections = new ArrayList<>();

    /**
     * Build a connection pool with many RabbitMQ instances
     *
     * @throws Exception
     */
    public void buildConnectionFactory() throws Exception {
        this.createConnection();
    }

    public void closeConnection(RabbitDomain rabbitDomain) throws Exception {
        try {
            if (rabbitDomain.getChannel() != null && rabbitDomain.getChannel().isOpen()) {
                rabbitDomain.getChannel().close();
            }
            if (rabbitDomain.getConnection() != null) {
                rabbitDomain.getConnection().close();

                this.rabbitConnections.remove(rabbitDomain);
                log.info("Encerrando conexao [" + rabbitDomain.getConnectionId() + "] por queda de channel...");
            } else {
                log.error("Falha na tentativa de encerramento da conexao [" + rabbitDomain.getConnectionId() + "]...");
            }
        } catch (Exception ex) {
            log.error("Erro no encerramento da conexao [" + rabbitDomain.getConnectionId() + "] por queda do channel...");
            throw new Exception("Erro no encerramento da conexao [" + rabbitDomain.getConnectionId() + "] por queda do channel...");
        }
    }

    public QueueInformation queueExists(String connectionId, String queue) throws Exception {
        RabbitDomain rabbitDomain = this.validateRabbitDomain(connectionId);
        try {
            Channel channel = this.channel(rabbitDomain);
            Queue.DeclareOk declareOk = channel.queueDeclarePassive(queue);
            if (declareOk != null) {
                return new QueueInformation(declareOk);
            }
        } catch (IOException ex) {
            try {
                if (rabbitDomain != null) {
                    this.closeConnection(rabbitDomain);
                    this.rabbitConnections.remove(rabbitDomain);
                    this.buildConnectionFactory();
                } else {
                    throw new Exception("Erro ao renovar conexoes com message broker, connectionId[" + connectionId + "]");
                }
            } catch (Exception e1) {
                log.error(ex.getMessage());
            }
            log.error("Queue [" + queue + "] nao faz parte do mensageiro no host [" + connectionId + "], erro = " + ex.getMessage());
        }
        return null;
    }

    public Long countConsumersQueue(String queue, String connectionId) throws Exception {
        try {
            RabbitDomain rabbitDomain = this.validateRabbitDomain(connectionId);
            Channel channel = this.channel(rabbitDomain);
            return channel.consumerCount(queue);
        } catch (IOException ex) {
            throw new IOException("Erro ao contar mensagens da fila, [erro] = " + ex.getMessage());
        }
    }

    public long countMessageQueue(String queue,
                                  String connectionId) throws Exception {
        try {
            RabbitDomain rabbitDomain = this.validateRabbitDomain(connectionId);
            Channel channel = this.channel(rabbitDomain);
            Queue.DeclareOk declareOk = channel.queueDeclarePassive(queue);

            if (declareOk.getQueue() != null) {
                return channel.messageCount(queue);
            } else {
                throw new Exception(String.format("Erro na contagem de mensagens para fila %s.", queue));
            }
        } catch (IOException ex) {
            throw new IOException("Erro ao contar mensagens da fila, [erro] = " + ex.getMessage());
        }
    }

    public int dropQueue(String queue, String connectionId) throws Exception {
        try {
            RabbitDomain rabbitDomain = this.validateRabbitDomain(connectionId);
            Channel channel = this.channel(rabbitDomain);
            Queue.DeleteOk removed = channel.queueDelete(queue);
            return removed.getMessageCount();
        } catch (IOException ex) {
            throw new IOException("Erro ao contar mensagens da fila, [erro] = " + ex.getMessage());
        }
    }

    public boolean existsConnection(String id) {
        for (RabbitDomain rabbitDomain : this.rabbitConnections) {
            return rabbitDomain.getConnectionId().equals(id);
        }
        return false;
    }

    public RabbitDomain rabbitDomain(String connectionId) throws IOException {
        RabbitDomain myDomain = null;
        for (RabbitDomain rabbitDomain : this.rabbitConnections) {
            if (rabbitDomain.getConnectionId().equals(connectionId)) {
                myDomain = rabbitDomain;
            }
            if (myDomain != null) break;
        }
        return myDomain;
    }

    private RabbitDomain createChannel(String connectionId) throws IOException {
        try {
            RabbitDomain rabbitDomain = this.rabbitDomain(connectionId);
            if (rabbitDomain != null) {
                if (rabbitDomain.getChannel() == null) {
                    log.info("Criando channel para connectionId = " + connectionId + "...");
                    Channel channel = rabbitDomain.getConnection().createChannel();
                    channel.basicRecover(true);
                    channel.basicQos(5, true);
                    rabbitDomain.setChannel(channel);
                }
                return rabbitDomain;
            }
            log.info("ConnectionId[" + connectionId + "] encerrada ou inexistente. Criando nova conexao com mensageiro...");
            return null;
        } catch (IOException ex) {
            throw new IOException(ex.getMessage());
        }
    }

    private RabbitDomain createRabbitDomain(String[] connectionDetails) {
        return new RabbitDomain("", null, null,
                connectionDetails[1],
                Integer.valueOf(connectionDetails[2]),
                connectionDetails[3],
                connectionDetails[4]);
    }

    private void createConnection() throws Exception {
        try {
            if (this.connectionFactory == null) this.connectionFactory = new ConnectionFactory();
            if (this.connections == null) this.connections = System.getenv("connections").split(",");

            Stream.of(connections).forEach(connection -> {
                String plainTextConnection = connection.replace("[", "").replace("]", "");
                String[] connectionDetails = plainTextConnection.split(";");

                log.info("Criando conexao para identificador = " + connectionDetails[0]);
                if (!existsConnection(connectionDetails[0].trim())) {
                    this.connectionFactory.setHost(connectionDetails[1].trim());
                    this.connectionFactory.setPort(Integer.parseInt(connectionDetails[2].trim()));
                    this.connectionFactory.setUsername(connectionDetails[3].trim());
                    this.connectionFactory.setPassword(connectionDetails[4].trim());
                    this.connectionFactory.setAutomaticRecoveryEnabled(true);

                    RabbitDomain rabbitDomain = this.createRabbitDomain(connectionDetails);
                    try {
                        Connection rabbitConnection = this.connectionFactory.newConnection();
                        rabbitConnection.setId(connectionDetails[0]);
                        rabbitDomain.setConnection(rabbitConnection);
                        rabbitDomain.setConnectionId(rabbitConnection.getId());

                        this.rabbitConnections.add(rabbitDomain);
                    } catch (IOException ex) {
                        log.error("Falha ao estabelecer conexao com message broker, servidor[" + rabbitDomain.getHost() + "]. Erro = " + ex.getMessage());
                    } catch (TimeoutException ex) {
                        log.error("Timeout na tentativa de conexao com message broker, servidor[" + rabbitDomain.getHost() + "]. Erro = " + ex.getMessage());
                    }
                } else {
                    log.info("ConnectionId[" + connectionDetails[0] + "] esta em uso...");
                }
            });
            log.info("Total de conexoes criadas => " + this.rabbitConnections.size());
        } catch (Exception ex) {
            log.error("Falha ao estabelecer conexao com message broker..., erro = " + ex.getMessage());
            throw new Exception("Falha ao estabelecer conexao com message broker..., erro = " + ex.getMessage());
        }
    }

    private Connection connection(RabbitDomain rabbitDomain) throws Exception {
        RabbitDomain myDomain = rabbitDomain;
        try {
            if (myDomain.getConnection() != null) {
                return myDomain.getConnection();
            }
            log.info("Recriando connection por inatividade para connectionId = " + rabbitDomain.getConnectionId() + "...");
            this.createConnection();//Renovando conexao
            myDomain = this.rabbitDomain(rabbitDomain.getConnectionId());
            return rabbitDomain.getConnection();
        } catch (Exception ex) {
            log.error("Falha na tentativa de renovar a conexao para connectionId = " + rabbitDomain.getConnectionId());
            throw new Exception("Falha na tentativa de renovar a conexao para connectionId = " + rabbitDomain.getConnectionId());
        }
    }

    private Channel channel(RabbitDomain rabbitDomain) throws Exception {
        RabbitDomain myDomain = rabbitDomain;
        try {
            if (myDomain.getChannel() != null) {
                return myDomain.getChannel();
            }
            log.info("Recriando channel por inatividade para connectionId = " + rabbitDomain.getConnectionId() + "...");
            Channel channel = myDomain.getConnection().createChannel();
            return channel;
        } catch (Exception ex) {
            log.error("Falha na tentativa de renovar o channel para connectionId = " + rabbitDomain.getConnectionId());
            throw new Exception("Falha na tentativa de renovar o channel para connectionId = " + rabbitDomain.getConnectionId());
        }
    }

    private RabbitDomain validateRabbitDomain(String idConnection) throws Exception {
        RabbitDomain rabbitDomain = this.createChannel(idConnection);
        if (rabbitDomain == null) {
            this.createConnection();
            rabbitDomain = this.createChannel(idConnection);
        }
        return rabbitDomain;
    }

    public RabbitDomain createDirectExchangeForQueue(String queueName,
                                                     String exchange,
                                                     String routingKey,
                                                     String idConnection) throws IOException, TimeoutException, Exception {
        try {
            RabbitDomain rabbitDomain = this.validateRabbitDomain(idConnection);
            HashMap<String, Object> args = new HashMap<>();
            args.put("x-queue-type", "classic");
            try {
                this.connection(rabbitDomain);

                Channel channel = this.channel(rabbitDomain);
                channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true);
                channel.queueDeclare(queueName, true, false, false, args);
                channel.queueBind(queueName, exchange, routingKey);
            } catch (IOException ex) {
                log.error("Falha no registro da \"queue\" usando \"DIRECT EXCHANGE\" para o servidor[" + rabbitDomain.getHost() + "]..., erro = " + ex.getMessage());
                throw new IOException("Falha no registro da \"queue\" usando \"DIRECT EXCHANGE\" para o servidor[" + rabbitDomain.getHost() + "]..., erro = " + ex.getMessage());
            }
            return rabbitDomain;
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }

    public RabbitDomain createDirectExchangeForQueue(String queueName,
                                                     String exchange,
                                                     String routingKey,
                                                     String idConnection,
                                                     HashMap<String, Object> args) throws IOException, TimeoutException, Exception {
        try {
            RabbitDomain rabbitDomain = this.validateRabbitDomain(idConnection);
            try {
                this.connection(rabbitDomain);

                Channel channel = this.channel(rabbitDomain);
                channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true);
                channel.queueDeclare(queueName, true, false, false, args);
                channel.queueBind(queueName, exchange, routingKey);
            } catch (IOException ex) {
                log.error("Falha no registro da \"queue\" usando \"DIRECT EXCHANGE\" para o servidor[" + rabbitDomain.getHost() + "]..., erro = " + ex.getMessage());
                throw new IOException("Falha no registro da \"queue\" usando \"DIRECT EXCHANGE\" para o servidor[" + rabbitDomain.getHost() + "]..., erro = " + ex.getMessage());
            }
            return rabbitDomain;
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }

    public RabbitDomain createDirectExchangeDelayedMessageForQueue(String queueName,
                                                                   String exchange,
                                                                   String routingKey,
                                                                   String idConnection) throws IOException, TimeoutException, Exception {
        try {
            RabbitDomain rabbitDomain = this.validateRabbitDomain(idConnection);
            HashMap<String, Object> args = new HashMap<>();
            args.put("x-queue-type", "classic");

            try {
                this.connection(rabbitDomain);
                Channel channel = this.channel(rabbitDomain);

                Map<String, Object> exchangeArgs = new HashMap<>();
                exchangeArgs.put("x-delayed-type", "direct");
                channel.exchangeDeclare(exchange, "x-delayed-message", true, false, exchangeArgs);
                channel.queueDeclare(queueName, true, false, false, args);
                channel.queueBind(queueName, exchange, routingKey);
            } catch (IOException ex) {
                log.error("Falha no registro da \"queue\" usando \"DIRECT DELAYED EXCHANGE\" para o servidor["
                        + rabbitDomain.getHost() + "]..., erro = " + ex.getMessage());
                throw new IOException("Falha no registro da \"queue\" usando \"DIRECT EXCHANGE\" para o servidor["
                        + rabbitDomain.getHost() + "]..., erro = " + ex.getMessage());
            }
            return rabbitDomain;
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }

    public RabbitDomain createDirectExchangeDelayedMessageForQueue(String queueName,
                                                                   String exchange,
                                                                   String routingKey,
                                                                   String idConnection,
                                                                   HashMap<String, Object> args) throws IOException, TimeoutException, Exception {
        try {
            RabbitDomain rabbitDomain = this.validateRabbitDomain(idConnection);

            try {
                this.connection(rabbitDomain);
                Channel channel = this.channel(rabbitDomain);

                Map<String, Object> exchangeArgs = new HashMap<>();
                exchangeArgs.put("x-delayed-type", "direct");
                channel.exchangeDeclare(exchange, "x-delayed-message", true, false, exchangeArgs);
                channel.queueDeclare(queueName, true, false, false, args);
                channel.queueBind(queueName, exchange, routingKey);
            } catch (IOException ex) {
                log.error("Falha no registro da \"queue\" usando \"DIRECT DELAYED EXCHANGE\" para o servidor["
                        + rabbitDomain.getHost() + "]..., erro = " + ex.getMessage());
                throw new IOException("Falha no registro da \"queue\" usando \"DIRECT EXCHANGE\" para o servidor["
                        + rabbitDomain.getHost() + "]..., erro = " + ex.getMessage());
            }
            return rabbitDomain;
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }


    public RabbitDomain sendTextMessage(String idConnection,
                                        String exchange,
                                        String routingKey,
                                        String data) throws Exception {
        try {
            RabbitDomain rabbitDomain = this.validateRabbitDomain(idConnection);
            Channel channel = this.channel(rabbitDomain);

            BasicProperties basicProperties = new BasicProperties().builder()
                    .deliveryMode(2)
                    .priority(1)
                    .appId(UUID.randomUUID().toString())
                    .timestamp(new Date(System.currentTimeMillis()))
                    .build();
            channel.basicPublish(exchange, routingKey, basicProperties, data.getBytes());
            return rabbitDomain;
        } catch (IOException ex) {
            throw new IOException(ex.getMessage());
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }

    public RabbitDomain sendTextMessage(String idConnection,
                                        String exchange,
                                        String routingKey,
                                        String data,
                                        Map<String, Object> headers) throws Exception {
        try {
            RabbitDomain rabbitDomain = this.validateRabbitDomain(idConnection);
            Channel channel = this.channel(rabbitDomain);

            BasicProperties basicProperties = new BasicProperties().builder()
                    .deliveryMode(2)
                    .priority(1)
                    .appId(UUID.randomUUID().toString())
                    .timestamp(new Date(System.currentTimeMillis()))
                    .headers(headers)
                    .build();
            channel.basicPublish(exchange, routingKey, basicProperties, data.getBytes());
            return rabbitDomain;
        } catch (IOException ex) {
            throw new IOException(ex.getMessage());
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }

    public RabbitDomain sendObjectMessage(String idConnection,
                                          String exchange,
                                          String routingKey,
                                          Object data) throws Exception {
        try {
            RabbitDomain rabbitDomain = this.validateRabbitDomain(idConnection);
            Channel channel = this.channel(rabbitDomain);
            byte[] message = new TreatMessageContent().convertToBytes(data);
            BasicProperties basicProperties = new BasicProperties().builder()
                    .deliveryMode(2)
                    .priority(1)
                    .appId(UUID.randomUUID().toString())
                    .timestamp(new Date(System.currentTimeMillis()))
                    .build();
            channel.basicPublish(exchange, routingKey, basicProperties, message);
            return rabbitDomain;
        } catch (IOException ex) {
            throw new IOException(ex.getMessage());
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }

    public RabbitDomain sendObjectMessage(String idConnection,
                                          String exchange,
                                          String routingKey,
                                          Object data,
                                          Map<String, Object> headers) throws Exception {
        try {
            RabbitDomain rabbitDomain = this.validateRabbitDomain(idConnection);
            Channel channel = this.channel(rabbitDomain);
            byte[] message = new TreatMessageContent().convertToBytes(data);
            BasicProperties basicProperties = new BasicProperties().builder()
                    .deliveryMode(2)
                    .priority(1)
                    .appId(UUID.randomUUID().toString())
                    .timestamp(new Date(System.currentTimeMillis()))
                    .headers(headers)
                    .build();
            channel.basicPublish(exchange, routingKey, basicProperties, message);
            return rabbitDomain;
        } catch (IOException ex) {
            throw new IOException(ex.getMessage());
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }
}
