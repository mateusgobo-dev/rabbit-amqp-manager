package model;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class RabbitDomain implements Serializable{
	private static final long serialVersionUID = 1L;
	private String connectionId;
	private Connection connection;
	private Channel channel;
	private String host;
	private Integer port;
	private String username;
	private String password;
}
