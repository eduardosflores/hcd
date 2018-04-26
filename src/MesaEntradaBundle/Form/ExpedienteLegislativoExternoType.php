<?php

namespace MesaEntradaBundle\Form;

use Ivory\CKEditorBundle\Form\Type\CKEditorType;
use Symfony\Component\Form\AbstractType;
use Symfony\Component\Form\Extension\Core\Type\DateType;
use Symfony\Component\Form\Extension\Core\Type\SubmitType;
use Symfony\Component\Form\Extension\Core\Type\TextareaType;
use Symfony\Component\Form\FormBuilderInterface;
use Symfony\Component\OptionsResolver\OptionsResolver;
use UtilBundle\Form\Type\BootstrapCollectionType;
use UtilBundle\Form\Type\Select2EntityType;

class ExpedienteLegislativoExternoType extends AbstractType {
	public function buildForm( FormBuilderInterface $builder, array $options ) {
		$builder
			->add( 'expediente' )
			->add( 'letra' )
			->add( 'periodoLegislativo',
				null,
				[
					'attr'     => [ 'class' => 'select2' ],
					'required' => true
				] )
			->add( 'extracto',
				TextareaType::class,
				[
					'attr' => [ 'rows' => 5 ]
				] )
			->add( 'texto',
				CKEditorType::class,
				[
					'required' => true,
					'config'   => array(
						'uiColor' => '#ffffff',
//						'height'  => '600px'
					),
					'attr'     => [ 'class' => 'texto_por_defecto' ]
				] )
//			->add( 'iniciadorParticular',
//				Select2EntityType::class,
//				[
//					'remote_route' => 'get_persona_por_dni',
//					'class'        => 'AppBundle\Entity\Persona',
//					'required'     => false,
//					'placeholder'  => 'Por DNI'
//
//				] )
			->add( 'dependencia',
				Select2EntityType::class,
				[
					'remote_route' => 'get_dependencias_por_nombre',
					'class'        => 'AppBundle\Entity\Dependencia',
					'required'     => false,
					'placeholder'  => 'Por Nombre'

				] )
			->add( 'fecha',
				DateType::class,
				array(
					'widget' => 'single_text',
					'html5'  => true
//					'format' => 'dd/MM/yyyy',
				) )
			->add( 'giros',
				BootstrapCollectionType::class,
				[
					'entry_type'   => GiroType::class,
					'allow_add'    => true,
					'allow_delete' => true,
					'by_reference' => false,
				] )
			->add( 'guardar',
				SubmitType::class,
				array(
					'attr' => array( 'class' => 'btn btn-primary' ),
				) );
	}

	public function configureOptions( OptionsResolver $resolver ) {
		$resolver->setDefaults( array(
			'data_class' => 'MesaEntradaBundle\Entity\Expediente',
		) );
	}

	public function getBlockPrefix() {
		return 'mesa_entrada_bundle_expediente_legislativo_externo_type';
	}
}