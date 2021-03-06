<?php

namespace MesaEntradaBundle\Repository;

use Doctrine\ORM\EntityRepository;

/**
 * IniciadorRepository
 *
 * This class was generated by the Doctrine ORM. Add your own custom
 * repository methods below.
 */
class IniciadorRepository extends EntityRepository {
	public function getCargosPorNombre( $apellido, $limit = 10, $returnArray = false ) {
		$qb = $this->createQueryBuilder( 'i' )
		           ->addSelect( 'p' )
		           ->addSelect( 'c' )
		           ->addSelect( 'cp' );
		$qb
			->join( 'i.cargoPersona', 'cp' )
			->join( 'cp.persona', 'p' )
			->join( 'cp.cargo', 'c' )
			->where( "upper(p.apellido) like upper(:apellido)" );
		$qb->setParameter( 'apellido', '%' . $apellido . '%' );

		$qb->setMaxResults( $limit );

		if ( $returnArray ) {
			return $qb->getQuery()->getArrayResult();
		} else {
			return $qb->getQuery()->getResult();
		}
	}

	public function getACargosPorNombre($apellido, $limit = 10) {

		$sql = "SELECT iniciador.id, persona.nombre AS nombre_persona, persona.apellido as apellido_persona, cargo.nombre AS cargo FROM cargo_persona cargo_persona
				LEFT JOIN persona persona ON cargo_persona.persona_id = persona.id
				LEFT JOIN cargo cargo on cargo_persona.cargo_id = cargo.id
				INNER JOIN iniciador iniciador on cargo_persona.id = iniciador.cargo_persona_id
				WHERE upper(persona.apellido) like upper('%$apellido%')
				AND iniciador.activo = true
				LIMIT $limit
				";

		$em   = $this->getEntityManager();
		$stmt = $em->getConnection()
		           ->prepare( $sql );
		$stmt->execute();
		$cargos = $stmt->fetchAll();

		return $cargos;
	}

	public function getACargosPorNombreLegacy($apellido, $limit = 10) {

		$sql = "SELECT iniciador.id, persona.nombre AS nombre_persona, persona.apellido as apellido_persona, cargo.nombre AS cargo FROM cargo_persona cargo_persona
				LEFT JOIN persona persona ON cargo_persona.persona_id = persona.id
				LEFT JOIN cargo cargo on cargo_persona.cargo_id = cargo.id
				INNER JOIN iniciador iniciador on cargo_persona.id = iniciador.cargo_persona_id
				WHERE upper(persona.apellido) like upper('%$apellido%')
				
				LIMIT $limit
				";

		$em   = $this->getEntityManager();
		$stmt = $em->getConnection()
		           ->prepare( $sql );
		$stmt->execute();
		$cargos = $stmt->fetchAll();

		return $cargos;
	}
}
