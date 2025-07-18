package model;

import com.rabbitmq.client.Channel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public record RabbitChannel(Integer index, Channel channel)
		implements Serializable{
	private static final long serialVersionUID = 1l;
}
