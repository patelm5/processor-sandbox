package io.github.patelm5.processorsandbox

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.support.DefaultKafkaHeaderMapper
import org.springframework.kafka.support.KafkaHeaderMapper
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.function.Function

@SpringBootApplication
@Configuration
class ProcessorSandboxApplication {

	@Bean
	fun process(): Function<KStream<Int, FootballStats>,KStream<Int, FootballStats>> {
		return Function<KStream<Int, FootballStats>,KStream<Int, FootballStats>>{ stream ->
			 stream.groupBy({ _, v -> v.gameId },
				Grouped.with(Serdes.Integer(), JsonSerde(FootballStats::class.java)))
				.reduce(FootballStatsReducer())
				.toStream()
		}
	}
	companion object {
		@JvmStatic
		fun main(args: Array<String>) {
			SpringApplication.run(ProcessorSandboxApplication::class.java, *args)
		}
	}
}


