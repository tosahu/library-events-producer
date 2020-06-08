package sks.springboot.kafka.libraryeventsproducer.controller;



import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import sks.springboot.kafka.libraryeventsproducer.domain.LibraryEvent;
import sks.springboot.kafka.libraryeventsproducer.domain.LibraryEventType;
import sks.springboot.kafka.libraryeventsproducer.producer.LibraryEventProducer;

@RestController
@Data
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class LibraryEventsController {
	
	@Autowired
	LibraryEventProducer libraryEventProducer;
	
	@GetMapping("/v1/hello")
	public String hello() {
		return "hello";
	}
	
	@PostMapping("/v1/libraryEvent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent){
	
		try {
			libraryEvent.setLibraryEventType(LibraryEventType.NEW);
			libraryEventProducer.sendLibraryEvent(libraryEvent);
			
		} catch (Exception e) {
			
			log.error("error is {}", e.getMessage());
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body(libraryEvent);
		}
		
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
		
	}

}
