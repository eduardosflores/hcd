<?php

namespace AppBundle\Repository;

/**
 * SesionRepository
 *
 * This class was generated by the Doctrine ORM. Add your own custom
 * repository methods below.
 */
class SesionRepository extends \Doctrine\ORM\EntityRepository {

	public function findQbUltimaSesion() {
		$qb = $this->createQueryBuilder( 's' );

		$qb->orderBy( 's.id', 'DESC' )
		   ->setMaxResults( 1 );

		return $qb;
	}

	public function findUltimaSesion() {
		$qb = $this->findQbUltimaSesion();

		return $qb->getQuery()->getScalarResult();
	}


}
