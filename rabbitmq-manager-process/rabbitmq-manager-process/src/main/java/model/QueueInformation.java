package model;

import com.rabbitmq.client.AMQP;

import java.io.Serializable;

public record QueueInformation(AMQP.Queue.DeclareOk declareOk)
	implements Serializable {

	private static final long serialVersionUID = 1L;
}
