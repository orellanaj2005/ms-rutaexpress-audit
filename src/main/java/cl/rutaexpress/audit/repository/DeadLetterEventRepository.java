package cl.rutaexpress.audit.repository;

import cl.rutaexpress.audit.domain.DeadLetterEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DeadLetterEventRepository extends JpaRepository<DeadLetterEvent, Long> {
}