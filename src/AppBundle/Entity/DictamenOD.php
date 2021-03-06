<?php

namespace AppBundle\Entity;

use Doctrine\ORM\Mapping as ORM;
use MesaEntradaBundle\Entity\Dictamen;
use MesaEntradaBundle\Entity\Expediente;
use UtilBundle\Entity\Base\BaseClass;
use Symfony\Bridge\Doctrine\Validator\Constraints\UniqueEntity;

/**
 * DictamenOD
 *
 * @ORM\Table(name="dictamen_o_d")
 * @ORM\Entity(repositoryClass="AppBundle\Repository\DictamenODRepository")
 * @UniqueEntity(
 *     fields={"expediente", "ordenDelDia"},
 *     errorPath="expediente",
 *     message="Este Dictámen ya existe en el OD"
 * )
 */
class DictamenOD extends BaseClass {
	/**
	 * @var int
	 *
	 * @ORM\Column(name="id", type="integer")
	 * @ORM\Id
	 * @ORM\GeneratedValue(strategy="AUTO")
	 */
	private $id;

	/**
	 * @var Expediente $expediente
	 *
	 * @ORM\ManyToOne(targetEntity="MesaEntradaBundle\Entity\Expediente")
	 * @ORM\JoinColumn(name="expediente_id", referencedColumnName="id", nullable=true)
	 */
	private $expediente;

	/**
	 * @var Dictamen $dictamen
	 *
	 * @ORM\ManyToOne(targetEntity="MesaEntradaBundle\Entity\Dictamen")
	 * @ORM\JoinColumn(name="dictamen_id", referencedColumnName="id", nullable=true)
	 */
	private $dictamen;

	/**
	 * @var OrdenDelDia $ordenDelDia
	 *
	 * @ORM\ManyToOne(targetEntity="AppBundle\Entity\OrdenDelDia", inversedBy="dictamenes")
	 * @ORM\JoinColumn(name="orden_del_dia_id", referencedColumnName="id", nullable=true)
	 */
	private $ordenDelDia;

	/**
	 * @var string
	 *
	 * @ORM\Column(name="extracto", type="text", nullable=true)
	 */
	private $extracto;

	/**
	 * @var $tieneTratamientoPreferencial
	 *
	 * @ORM\Column(name="tiene_tratamiento_preferencial", type="boolean", nullable=true)
	 */
	private $tieneTratamientoPreferencial;

	/**
	 * @var $incorporadoEnSesion
	 *
	 * @ORM\Column(name="incorporado_en_sesion", type="boolean", nullable=true)
	 */
	private $incorporadoEnSesion;

	/**
	 * @var $aprobadoSobreTabla
	 *
	 * @ORM\Column(name="aprobado_sobre_tabla", type="boolean", nullable=true)
	 */
	private $aprobadoSobreTabla;

	/**
	 * @var $vueltaAComision
	 *
	 * @ORM\Column(name="vuelta_acomision", type="boolean", nullable=true)
	 */
	private $vueltaAComision;


	/**
	 * Get id
	 *
	 * @return int
	 */
	public function getId() {
		return $this->id;
	}

	/**
	 * Set fechaCreacion
	 *
	 * @param \DateTime $fechaCreacion
	 *
	 * @return DictamenOD
	 */
	public function setFechaCreacion( $fechaCreacion ) {
		$this->fechaCreacion = $fechaCreacion;

		return $this;
	}

	/**
	 * Set fechaActualizacion
	 *
	 * @param \DateTime $fechaActualizacion
	 *
	 * @return DictamenOD
	 */
	public function setFechaActualizacion( $fechaActualizacion ) {
		$this->fechaActualizacion = $fechaActualizacion;

		return $this;
	}

	/**
	 * @return Dictamen
	 */
	public function getDictamen() {
		return $this->dictamen;
	}

	/**
	 * @param Dictamen $dictamen
	 */
	public function setDictamen( Dictamen $dictamen ) {
		$this->dictamen = $dictamen;
	}

	/**
	 * Set ordenDelDia
	 *
	 * @param \AppBundle\Entity\OrdenDelDia $ordenDelDia
	 *
	 * @return DictamenOD
	 */
	public function setOrdenDelDia( \AppBundle\Entity\OrdenDelDia $ordenDelDia = null ) {
		$this->ordenDelDia = $ordenDelDia;

		return $this;
	}

	/**
	 * Get ordenDelDia
	 *
	 * @return \AppBundle\Entity\OrdenDelDia
	 */
	public function getOrdenDelDia() {
		return $this->ordenDelDia;
	}

	/**
	 * Set creadoPor
	 *
	 * @param \UsuariosBundle\Entity\Usuario $creadoPor
	 *
	 * @return DictamenOD
	 */
	public function setCreadoPor( \UsuariosBundle\Entity\Usuario $creadoPor = null ) {
		$this->creadoPor = $creadoPor;

		return $this;
	}

	/**
	 * Set actualizadoPor
	 *
	 * @param \UsuariosBundle\Entity\Usuario $actualizadoPor
	 *
	 * @return DictamenOD
	 */
	public function setActualizadoPor( \UsuariosBundle\Entity\Usuario $actualizadoPor = null ) {
		$this->actualizadoPor = $actualizadoPor;

		return $this;
	}

	/**
	 * Set extracto
	 *
	 * @param string $extracto
	 *
	 * @return DictamenOD
	 */
	public function setExtracto( $extracto ) {
		$this->extracto = $extracto;

		return $this;
	}

	/**
	 * Get extracto
	 *
	 * @return string
	 */
	public function getExtracto() {
		return $this->extracto;
	}

	/**
	 * Set tieneTratamientoPreferencial
	 *
	 * @param boolean $tieneTratamientoPreferencial
	 *
	 * @return DictamenOD
	 */
	public function setTieneTratamientoPreferencial( $tieneTratamientoPreferencial ) {
		$this->tieneTratamientoPreferencial = $tieneTratamientoPreferencial;

		return $this;
	}

	/**
	 * Get tieneTratamientoPreferencial
	 *
	 * @return boolean
	 */
	public function getTieneTratamientoPreferencial() {
		return $this->tieneTratamientoPreferencial;
	}

	/**
	 * @return mixed
	 */
	public function getIncorporadoEnSesion() {
		return $this->incorporadoEnSesion;
	}

	/**
	 * @param mixed $incorporadoEnSesion
	 */
	public function setIncorporadoEnSesion( $incorporadoEnSesion ) {
		$this->incorporadoEnSesion = $incorporadoEnSesion;
	}

	/**
	 * @return mixed
	 */
	public function getAprobadoSobreTabla() {
		return $this->aprobadoSobreTabla;
	}

	/**
	 * @param mixed $aprobadoSobreTabla
	 */
	public function setAprobadoSobreTabla( $aprobadoSobreTabla ) {
		$this->aprobadoSobreTabla = $aprobadoSobreTabla;
	}

	/**
	 * @return mixed
	 */
	public function getVueltaAComision() {
		return $this->vueltaAComision;
	}

	/**
	 * @param mixed $vueltaAComision
	 */
	public function setVueltaAComision( $vueltaAComision ) {
		$this->vueltaAComision = $vueltaAComision;
	}


}
