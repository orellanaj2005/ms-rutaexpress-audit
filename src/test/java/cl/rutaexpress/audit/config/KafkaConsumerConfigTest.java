package cl.rutaexpress.audit.config;

import org.junit.jupiter.api.Test;
import org.springframework.util.backoff.BackOffExecution;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConsumerConfigTest {

    @Test
    void backoffEsExponencial1s2s4sYLuegoSeAcaba() {
        BackOffExecution exec = KafkaConsumerConfig.retryBackOff().start();

        assertThat(exec.nextBackOff()).isEqualTo(1000L);
        assertThat(exec.nextBackOff()).isEqualTo(2000L);
        assertThat(exec.nextBackOff()).isEqualTo(4000L);
        assertThat(exec.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }
}
