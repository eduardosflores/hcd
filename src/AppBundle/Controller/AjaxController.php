<?php

namespace AppBundle\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;

class AjaxController extends Controller {
	public function getAjaxDefaultAction( Request $request ) {
		$value = strtoupper( $request->get( 'term' ) );
		//$value = $request->get('term');
		$class        = $request->get( 'class' );
		$property     = $request->get( 'property' );
		$searchMethod = $request->get( 'search_method' );
		$em           = $this->getDoctrine()->getManagerForClass( $class );

		$entities = $em->getRepository( $class )->$searchMethod( $value );

		$json = array();

		if ( ! count( $entities ) ) {
			$json[] = array(
				'label' => 'No se encontraron coincidencias',
				'value' => ''
			);
		} else {

			foreach ( $entities as $entity ) {
				$json[] = array(
					'id'   => $entity['id'],
					//'label' => $entity[$property],
					'text' => $entity[ $property ]
				);
			}
		}

		return new JsonResponse( $json );
	}

	public function getPersonaPorDniAction( Request $request ) {

		$em = $this->getDoctrine();

		$value    = $request->get( 'q' );
		$limit    = $request->get( 'page_limit' );
		$entities = $em->getRepository( 'AppBundle:Persona' )->getPersonaPorDni( $value, $limit, true );

		$json = array();

		if ( ! count( $entities ) ) {
			$json[] = array(
				'text' => 'No se encontraron coincidencias',
				'id'   => ''
			);
		} else {

			foreach ( $entities as $entity ) {
				$json[] = array(
					'id'   => $entity['id'],
					//'label' => $entity[$property],
					'text' => $entity['nombre'] . ' ' . $entity['apellido']
				);
			}
		}

		return new JsonResponse( $json );
	}

	public function getCargosPorNombreAction( Request $request ) {

		$em = $this->getDoctrine();

		$value    = $request->get( 'q' );
		$limit    = $request->get( 'page_limit' );
		$entities = $em->getRepository( 'MesaEntradaBundle:Iniciador' )->getCargosPorNombre( $value, $limit, true );

		$json = array();

		if ( ! count( $entities ) ) {
			$json[] = array(
				'text' => 'No se encontraron coincidencias',
				'id'   => ''
			);
		} else {

			foreach ( $entities as $entity ) {
				$json[] = array(
					'id'   => $entity['id'],
					//'label' => $entity[$property],
					'text' => $entity['persona']['cargoPersona'][0]['cargo']['nombre'] . ' ' . $entity['persona']['nombre'] . ' ' . $entity['persona']['apellido']
				);
			}
		}

		return new JsonResponse( $json );
	}
}