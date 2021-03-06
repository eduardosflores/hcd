<?php

namespace AppBundle\Controller;

use AppBundle\Entity\DictamenOD;
use AppBundle\Entity\Sesion;
use AppBundle\Form\AltaDictamenType;
use AppBundle\Form\AsignarDictamenAExpteType;
use AppBundle\Form\CrearDictamenType;
use AppBundle\Form\DictamenODIncorporarType;
use AppBundle\Form\Filter\DictamenFilterType;
use Doctrine\Common\Collections\ArrayCollection;
use MesaEntradaBundle\Entity\Dictamen;
use Symfony\Bundle\FrameworkBundle\Controller\Controller;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;

class DictamenController extends Controller {
	public function indexAction( Request $request ) {
		$em = $this->getDoctrine()->getManager();

		$filterType = $this->createForm( DictamenFilterType::class,
			null,
			[
				'method' => 'GET'
			] );


		$filterType->handleRequest( $request );


		if ( $filterType->get( 'buscar' )->isClicked() ) {

			$dictamenes = $em->getRepository( 'MesaEntradaBundle:Dictamen' )->getQbBuscar( $filterType->getData() );
		} else {

			$dictamenes = $em->getRepository( 'MesaEntradaBundle:Dictamen' )->getQbAll();
		}


		$paginator  = $this->get( 'knp_paginator' );
		$dictamenes = $paginator->paginate(
			$dictamenes,
			$request->query->get( 'page', 1 )/* page number */,
			10/* limit per page */
		);

		return $this->render( 'dictamen/index.html.twig',
			[
				'filter_type' => $filterType->createView(),
				'dictamenes'  => $dictamenes
			] );
	}

	public function crearAction( Request $request ) {

		$esPresidenteComision = $this->getUser()->getPersona()->esPresidenteComision();

		if ( ! $esPresidenteComision ) {
			$this->get( 'session' )->getFlashBag()->add(
				'warning',
				'Ud. no es Presidente de una Comisión'
			);

			return $this->redirectToRoute( 'dictamen_index' );
		}

		$em = $this->getDoctrine()->getManager();

		$dictamen = new Dictamen();

		$form = $this->createForm( CrearDictamenType::class, $dictamen );

		$form->handleRequest( $request );

		if ( $form->isSubmitted() && $form->isValid() ) {

			$dictamen->setPresidenteComision( $esPresidenteComision );

			$em->persist( $dictamen );
			$em->flush();
			$this->get( 'session' )->getFlashBag()->add(
				'success',
				'Dictamen creado correctamente'
			);

			return $this->redirectToRoute( 'dictamen_index' );
		}

		return $this->render( 'dictamen/crear.html.twig',
			[
				'form' => $form->createView()
			] );
	}

	public function altaDictamenAnteriorAction( Request $request ) {

		$dictamen = new Dictamen();

		$form = $this->createForm( AltaDictamenType::class, $dictamen );

		$form->handleRequest( $request );

		if ( $form->isSubmitted() && $form->isValid() ) {
			$em = $this->getDoctrine()->getManager();

			$dictamen->getExpediente()->setBorrador( false );
			$tipoExpediente = $em->getRepository( 'MesaEntradaBundle:TipoExpediente' )->findOneBySlug( 'externo' );
			$dictamen->getExpediente()->setTipoExpediente( $tipoExpediente );

			$em->persist( $dictamen );
			$em->flush();
			$this->get( 'session' )->getFlashBag()->add(
				'success',
				'Dictamen creado correctamente'
			);

			return $this->redirectToRoute( 'dictamen_index' );
		}

		return $this->render( 'dictamen/alta.html.twig',
			[
				'form' => $form->createView()
			] );
	}

	public function editarDictamenAnteriorAction( Request $request, $id ) {

		if ( ! $this->get( 'security.authorization_checker' )->isGranted( 'ROLE_LEGISLATIVO' ) ) {
			$this->get( 'session' )->getFlashBag()->add(
				'warning',
				'No tiene permisos para modificar un Dictamen.'
			);

			return $this->redirectToRoute( 'app_homepage' );
		}

		$em = $this->getDoctrine()->getManager();

		$dictamen   = $em->getRepository( Dictamen::class )->find( $id );
		$expediente = $dictamen->getExpediente();

		$firmantesOriginales = new ArrayCollection();

		// Create an ArrayCollection of the current Tag objects in the database
		foreach ( $dictamen->getFirmantes() as $firmanteDictamen ) {
			$firmantesOriginales->add( $firmanteDictamen );
		}

		$anexosOriginales = new ArrayCollection();

		// Create an ArrayCollection of the current Tag objects in the database
		foreach ( $dictamen->getAnexos() as $anexo ) {
			$anexosOriginales->add( $anexo );
		}

		$form = $this->createForm( AltaDictamenType::class, $dictamen );

		$form->handleRequest( $request );

		if ( $form->isSubmitted() && $form->isValid() ) {

			foreach ( $firmantesOriginales as $firmanteDictamen ) {
				if ( false === $dictamen->getFirmantes()->contains( $firmanteDictamen ) ) {

					$firmanteDictamen->setDictamen( null );

					$em->persist( $firmanteDictamen );

					$em->remove( $firmanteDictamen );
				}
			}

			foreach ( $anexosOriginales as $anexo ) {
				if ( false === $dictamen->getAnexos()->contains( $anexo ) ) {

					$anexo->setDictamen( null );

					$em->persist( $anexo );

					$em->remove( $anexo );
				}
			}

			$em->flush();
			$this->get( 'session' )->getFlashBag()->add(
				'success',
				'Dictamen modificado correctamente'
			);

			return $this->redirectToRoute( 'dictamen_editar', [ 'id' => $id ] );
		}

		return $this->render( ':dictamen:editar.html.twig',
			[
				'form'     => $form->createView(),
				'dictamen' => $dictamen
			] );
	}

	public function verDictamenAction( Request $request, $id ) {

		$em = $this->getDoctrine()->getManager();

		$dictamen = $em->getRepository( 'MesaEntradaBundle:Dictamen' )->find( $id );


		return $this->render( ':dictamen:ver.html.twig',
			[
				'dictamen' => $dictamen
			] );
	}

	public function imprimirDictamenAction( Request $request, $id ) {
		$title = 'Dictamen';

		$em = $this->getDoctrine()->getManager();

		$dictamen = $em->getRepository( 'MesaEntradaBundle:Dictamen' )->find( $id );

		if ( ! $dictamen->getPeriodoLegislativo() ) {
			$this->get( 'session' )->getFlashBag()->add(
				'error',
				'El dictamen no tiene periodo legislativo asignado'
			);

			return $this->redirectToRoute( 'dictamen_ver', [ 'id' => $dictamen->getId() ] );
		}

		$header = null;

		$header = $this->renderView( ':default:membrete.pdf.twig',
			[
				"periodo"  => $dictamen->getPeriodoLegislativo(),
				'dictamen' => $dictamen,
			] );


		$footer = $this->renderView( ':default:pie_pagina.pdf.twig' );

		$html = $this->renderView( 'dictamen/dictamen.pdf.twig',
			[
				'dictamen' => $dictamen,
				'title'    => $title,
			]
		);

//        return new Response($html);

		return new Response(
			$this->get( 'knp_snappy.pdf' )->getOutputFromHtml( $html,
				array(
					'page-size'      => 'Legal',
					'margin-top'     => "7cm",
					'margin-bottom'  => "2cm",
					'header-html'    => $header,
					'header-spacing' => 6,
					'footer-spacing' => 5,
					'footer-html'    => $footer,
				)
			)
			, 200, array(
				'Content-Type'        => 'application/pdf',
				'Content-Disposition' => 'inline; filename="' . $title . '.pdf"'
			)
		);
	}

	public function asignarAExpteAction( Request $request ) {
		$dictamen = new Dictamen();

		$form = $this->createForm( AsignarDictamenAExpteType::class, $dictamen );

		$form->handleRequest( $request );

		if ( $form->isSubmitted() && $form->isValid() ) {
			$em = $this->getDoctrine()->getManager();

			$expediente = $form->get( "expediente" )->getData();
			$dictamen->setExpediente( $expediente );

			$em->persist( $dictamen );
			$em->flush();
			$this->get( 'session' )->getFlashBag()->add(
				'success',
				'Dictamen creado correctamente'
			);

			return $this->redirectToRoute( 'dictamen_index' );
		}

		return $this->render( 'dictamen/asignar_a_expte.html.twig',
			[
				'form' => $form->createView()
			] );
	}

	public function incorporarDictamenesEnSesionAction( Request $request ) {
		$em         = $this->getDoctrine()->getManager();
		$dictamenOD = new DictamenOD();
		$dictamen   = new Dictamen();
		$dictamenOD->setDictamen( $dictamen );

		$sesionQb = $em->getRepository( Sesion::class )->findQbUltimaSesion();

		$sesion = null;


		if ( ! $sesionQb->getQuery()->getResult() ) {
			$this->get( 'session' )->getFlashBag()->add(
				'warning',
				'No hay una Sesión Activa Creada'
			);
		} else {
			$sesion = $sesionQb->getQuery()->getSingleResult();
		}

		if ( $sesion ) {

			if ( $sesion->getBae()->last() ) {
				$dictamenOD->setOrdenDelDia( $sesion->getOd()->last() );
			} else {
				$this->get( 'session' )->getFlashBag()->add(
					'warning',
					'El Plan de labor aun no está conformado'
				);

				return $this->redirectToRoute( 'app_homepage' );
			}
		}

//		$dictamenOD->setExtracto( $dictamen->getExtracto() );


		$form = $this->createForm( DictamenODIncorporarType::class, $dictamenOD );

		$form->handleRequest( $request );


		if ( $form->isSubmitted() && $form->isValid() ) {

			$dictamenOD->setIncorporadoEnSesion( true );

			$expediente = $form->get( "dictamen" )->get("expediente")->getData();

			$dictamen->setExpediente( $expediente );

			$em->persist( $dictamen );
			$em->persist( $dictamenOD );
			$em->flush();

			$this->get( 'session' )->getFlashBag()->add(
				'success',
				'Dictamen ingresado correctamente'
			);

			return $this->redirectToRoute( 'dictamen_index' );
		}

		return $this->render( 'dictamenod/incorporar_en_sesion.html.twig',
			[
				'form'     => $form->createView(),
				'dictamen' => $dictamen,
			] );
	}
}