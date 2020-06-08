package sks.springboot.kafka.libraryeventsproducer.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import sks.springboot.kafka.libraryeventsproducer.domain.LibraryEvent;

@Component
@Slf4j
public class LibraryEventProducer {

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	@Autowired
	ObjectMapper objectMapper;

	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		ListenableFuture<SendResult<Integer, String>> listebableFuture = kafkaTemplate.sendDefault(key, value);
		listebableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key,value,result);

			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key,value,ex);

			}
		});

	}

	protected void handleFailure(Integer key, String value, Throwable ex) {
		
		log.error("Error occurs during sending message {}",ex.getCause());
		try {
			throw ex;
		}catch(Throwable e) {
			log.error("Error in Failure {}",e.getLocalizedMessage());
		}
		
		
	}

	protected void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message Successfully Sent for key : {} and value : {} with partition : {} to topic : {}", key,value,result.getRecordMetadata().partition(), result.getRecordMetadata().topic());		
	}
}