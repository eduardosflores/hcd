<?php

namespace AppBundle\Form;

use Ivory\CKEditorBundle\Form\Type\CKEditorType;
use Symfony\Component\Form\AbstractType;
use Symfony\Component\Form\Extension\Core\Type\TextType;
use Symfony\Component\Form\FormBuilderInterface;
use Symfony\Component\OptionsResolver\OptionsResolver;
use Symfony\Component\Validator\Constraints\Valid;

class AltaExpedienteDictamenType extends AbstractType {
	public function buildForm( FormBuilderInterface $builder, array $options ) {

		$builder
			->add( 'tipoProyecto',
				null,
				[
					'required'    => false,
					'placeholder' => 'Seleccionar',
					'attr'        => [ 'class' => 'tipo-proyecto select2' ]
				] )
			->add( 'nota' )
			->add( 'expediente',
				TextType::class,
				[
					'required' => true
				] )
			->add( 'letra',
				TextType::class,
				[
					'required' => true
				] )
			->add( 'periodoLegislativo',
				null,
				[
					'attr' => [ 'class' => 'select2' ]
				] )
			->add( 'extractoDictamen',
				CKEditorType::class,
				[
					'required' => true,
					'config'   => array(
						'uiColor' => '#ffffff',
//						'height'  => '600px'
					),
					'attr'     => [ 'class' => 'texto_por_defecto' ]
				] )
		;

	}

	public function configureOptions( OptionsResolver $resolver ) {
		$resolver->setDefaults( array(
			'data_class' => 'MesaEntradaBundle\Entity\Expediente',
			'constraints' => new Valid()
		) );
	}

	public function getBlockPrefix() {
		return 'app_bundle_alta_expediente_dictamen_type';
	}
}
