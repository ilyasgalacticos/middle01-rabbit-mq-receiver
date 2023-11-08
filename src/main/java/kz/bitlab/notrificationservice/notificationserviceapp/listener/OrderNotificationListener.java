package kz.bitlab.notrificationservice.notificationserviceapp.listener;

import kz.bitlab.notrificationservice.notificationserviceapp.dto.OrderDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderNotificationListener {

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "almaty_orders_queue",
                arguments = {
                    @Argument(name = "x-dead-letter-exchange", value = "dlx"),
                    @Argument(name = "x-dead-letter-routing-key", value = "dlx.almaty_orders")
                }
            ),
            exchange = @Exchange(value = "${mq.order.topic.exchange}",
                    type = ExchangeTypes.TOPIC),
            key = "order.#"
    ))
    public void receiveAlmatyOrder(OrderDTO orderDTO){
        log.info("Received order ORDER : {}", orderDTO);
        try{
            processOrder(orderDTO);
        }catch (Exception e){
            log.error("Error on processing order : {}, Error : {}", orderDTO, e.getMessage());
            throw e;
        }
        System.out.println("Received order ORDER : " + orderDTO);
    }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "customer_update_queue"),
            exchange = @Exchange(value = "${mq.order.fanout.exchange}",
                    type = ExchangeTypes.FANOUT),
            key = ""
    ))
    public void receiveCustomerOrderStatusUpdate(OrderDTO orderDTO){
        log.info("Customer notification ORDER : {}, status updated: {}", orderDTO, orderDTO.getStatus());
        System.out.println("Customer notification ORDER : " + orderDTO + " status updated : " + orderDTO.getStatus());
    }


    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "courier_update_queue"),
            exchange = @Exchange(value = "${mq.order.fanout.exchange}",
                    type = ExchangeTypes.FANOUT),
            key = ""
    ))
    public void receiveCourierOrderStatusUpdate(OrderDTO orderDTO){
        log.info("Courier notification ORDER : {}, status updated: {}", orderDTO, orderDTO.getStatus());
        System.out.println("Courier notification ORDER : " + orderDTO + " status updated : " + orderDTO.getStatus());
    }

    private void processOrder(OrderDTO orderDTO){
        if(true){
            throw new RuntimeException("Failed to process order");
        }
    }
}