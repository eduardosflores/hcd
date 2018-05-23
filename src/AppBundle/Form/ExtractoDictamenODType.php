<?php

namespace AppBundle\Form;

use Ivory\CKEditorBundle\Form\Type\CKEditorType;
use Symfony\Component\Form\AbstractType;
use Symfony\Component\Form\FormBuilderInterface;
use Symfony\Component\OptionsResolver\OptionsResolver;

class ExtractoDictamenODType extends AbstractType {
	/**
	 * {@inheritdoc}
	 */
	public function buildForm( FormBuilderInterface $builder, array $options ) {
		$builder
			->add( 'extracto',
				CKEditorType::class,
				[
					'required' => true,
					'config'   => array(
						'uiColor' => '#ffffff',
					)
				] );
	}

	/**
	 * {@inheritdoc}
	 */
	public function configureOptions( OptionsResolver $resolver ) {
		$resolver->setDefaults( array(
			'data_class' => 'AppBundle\Entity\DictamenOD',
		) );
	}

	/**
	 * {@inheritdoc}
	 */
	public function getBlockPrefix() {
		return 'appbundle_extracto_dictamen_ods';
	}


}
