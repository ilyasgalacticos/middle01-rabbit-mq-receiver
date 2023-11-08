package kz.bitlab.notrificationservice.notificationserviceapp.config;

import kz.bitlab.notrificationservice.notificationserviceapp.dto.OrderDTO;
import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitConfig {

    @Bean
    public Jackson2JsonMessageConverter producerJackson2MessageConverter() {
        Jackson2JsonMessageConverter messageConverter = new Jackson2JsonMessageConverter();
        messageConverter.setClassMapper(classMapper());
        return messageConverter;
    }

    @Bean
    public DefaultClassMapper classMapper() {
        DefaultClassMapper classMapper = new DefaultClassMapper();
        Map<String, Class<?>> idClassMapping = new HashMap<>();
        idClassMapping.put("kz.bitlab.notrificationservice.notificationserviceapp.dto.OrderDTO", OrderDTO.class);
        classMapper.setIdClassMapping(idClassMapping);
        return classMapper;
    }


    @Bean
    public Queue deadLetterQueue(){
        return QueueBuilder.durable("almaty_orders_queue.dlq").build();
    }

    @Bean
    public TopicExchange deadLetterExchange(){
        return ExchangeBuilder.topicExchange("dlx").durable(true).build();
    }

    @Bean
    public Binding DLQbinding(){
        return BindingBuilder
                .bind(deadLetterQueue())
                .to(deadLetterExchange())
                .with("dlx.almaty_orders");
    }
}
