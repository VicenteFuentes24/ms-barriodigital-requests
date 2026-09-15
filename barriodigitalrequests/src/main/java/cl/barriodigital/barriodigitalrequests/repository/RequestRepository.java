package cl.barriodigital.barriodigitalrequests.repository;

import cl.barriodigital.barriodigitalrequests.model.RequestEntity;
import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import java.util.List;

public interface RequestRepository extends JpaRepository<RequestEntity, Long>, JpaSpecificationExecutor<RequestEntity> {

    List<RequestEntity> findByStatus(RequestStatus status);
}
