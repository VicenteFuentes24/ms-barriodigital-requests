package cl.barriodigital.barriodigitalrequests.repository;

import cl.barriodigital.barriodigitalrequests.model.RequestEntity;
import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import java.util.List;
import java.util.Optional;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface RequestRepository extends JpaRepository<RequestEntity, Long>, JpaSpecificationExecutor<RequestEntity> {

    List<RequestEntity> findByStatus(RequestStatus status);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select r from RequestEntity r where r.id = :id")
    Optional<RequestEntity> findByIdForUpdate(@Param("id") Long id);
}
