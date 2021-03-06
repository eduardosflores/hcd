<?php

namespace AppBundle\Form;

use Symfony\Component\Form\AbstractType;
use Symfony\Component\Form\FormBuilderInterface;
use Symfony\Component\OptionsResolver\OptionsResolver;
use UtilBundle\Form\Type\Select2EntityType;
use Symfony\Component\Validator\Constraints\Valid;

class ProyectoBAEType extends AbstractType {
	/**
	 * {@inheritdoc}
	 */
	public function buildForm( FormBuilderInterface $builder, array $options ) {
		$builder
			->add( 'expediente',
				Select2EntityType::class,
				[
					'remote_route' => 'get_proyectos_bae',
					'class'        => 'MesaEntradaBundle\Entity\Expediente',
					'required'     => false,
					'placeholder'  => 'Por Expte'

				] )
			->add( 'esInformeDem',
				null,
				[
					'label' => 'Informe DEM'
				] );
	}

	/**
	 * {@inheritdoc}
	 */
	public function configureOptions( OptionsResolver $resolver ) {
		$resolver->setDefaults( array(
			'data_class'  => 'AppBundle\Entity\ProyectoBAE',
			'constraints' => new Valid()
		) );
	}

	/**
	 * {@inheritdoc}
	 */
	public function getBlockPrefix() {
		return 'appbundle_proyectobae';
	}


}
