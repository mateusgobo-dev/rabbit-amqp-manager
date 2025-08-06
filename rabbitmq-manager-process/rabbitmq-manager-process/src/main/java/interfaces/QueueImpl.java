package interfaces;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import connectors.RabbitMQConnection;
import model.RabbitDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;

@FunctionalInterface
public interface QueueImpl {
    String msgException = "Falha no registro da \"queue\" usando \"EXCHANGE\" para o servidor[ %s]...., erro = %s";
    Logger log = LoggerFactory.getLogger(RabbitMQConnection.class.getName());
    RabbitDomain addBrokerQueue(String queueName,
                        String exchange,
                        String routingKey,
                        RabbitDomain rabbitDomain,
                        Channel channel,
                        BuiltinExchangeType builtinExchangeType);

    QueueImpl buildQueue = (queueName, exchange, routingKey, rabbitDomain, channel, builtinExchangeType) -> {
         try {
             HashMap<String, Object> args = new HashMap<>();
             args.put("x-queue-type", "classic");
             args.put("x-dead-letter-exchange",  "DLX_"+exchange);
             args.put("x-dead-letter-routing-key", "DLX_"+routingKey);
             try {
                 channel.exchangeDeclare(exchange, (Objects.nonNull(builtinExchangeType) ? builtinExchangeType :  BuiltinExchangeType.DIRECT), true);
                 channel.exchangeDeclare("DLX_"+exchange, (Objects.nonNull(builtinExchangeType) ? builtinExchangeType :  BuiltinExchangeType.DIRECT), true);
                 channel.queueDeclare(queueName, true, false, false, args);
                 channel.queueDeclare("DLX_"+queueName, true, false, false, args);
                 channel.queueBind(queueName, exchange, routingKey);
                 channel.queueBind("DLX_"+queueName, "DLX_"+exchange, "DLX_"+routingKey);
             } catch (IOException ex) {
                 log.error(String.format(msgException,rabbitDomain.getHost(), ex.getMessage()));
                 throw new IOException(String.format(msgException,rabbitDomain.getHost(), ex.getMessage()));
             }
             return rabbitDomain;
         } catch (Exception ex) {
             throw new RuntimeException(ex.getMessage());
         }
    };
}
