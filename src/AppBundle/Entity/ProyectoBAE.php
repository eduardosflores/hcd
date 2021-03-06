<?php

namespace AppBundle\Entity;

use Doctrine\ORM\Mapping as ORM;
use MesaEntradaBundle\Entity\Expediente;
use MesaEntradaBundle\Entity\Giro;
use UtilBundle\Entity\Base\BaseClass;
use Symfony\Bridge\Doctrine\Validator\Constraints\UniqueEntity;

/**
 * ProyectoBAE
 *
 * @ORM\Table(name="proyecto_b_a_e")
 * @ORM\Entity(repositoryClass="AppBundle\Repository\ProyectoBAERepository")
 * @UniqueEntity(
 *     fields={"expediente", "boletinAsuntoEntrado"},
 *     errorPath="expediente",
 *     message="Este expediente ya existe en el BAE"
 * )
 */
class ProyectoBAE extends BaseClass {
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
	 * @var BoletinAsuntoEntrado $boletinAsuntoEntrado
	 *
	 * @ORM\ManyToOne(targetEntity="AppBundle\Entity\BoletinAsuntoEntrado", inversedBy="proyectos")
	 * @ORM\JoinColumn(name="boletin_asunto_entrado_id", referencedColumnName="id", nullable=true)
	 */
	private $boletinAsuntoEntrado;

	/**
	 * @var $informeDem
	 *
	 * @ORM\Column(name="es_informe_dem", type="boolean", nullable=true)
	 */
	private $esInformeDem;

	/**
	 * @var string
	 *
	 * @ORM\Column(name="extracto", type="text", nullable=true)
	 */
	private $extracto;

    /**
     * @var
     *
     * @ORM\OneToMany(targetEntity="MesaEntradaBundle\Entity\Giro", mappedBy="proyectoBae", cascade={"persist", "remove"})
     *
     */
    private $giros;

	/**
	 * @var $incorporadoEnSesion
	 *
	 * @ORM\Column(name="incorporado_en_sesion", type="boolean", nullable=true)
	 */
	private $incorporadoEnSesion;

	/**
	 * @var $tratamientoSobretabla
	 *
	 * @ORM\Column(name="tratamiento_sobretabla", type="boolean", nullable=true)
	 */
	private $tratamientoSobretabla;


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
	 * @return ProyectoBAE
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
	 * @return ProyectoBAE
	 */
	public function setFechaActualizacion( $fechaActualizacion ) {
		$this->fechaActualizacion = $fechaActualizacion;

		return $this;
	}

	/**
	 * Set expediente
	 *
	 * @param \MesaEntradaBundle\Entity\Expediente $expediente
	 *
	 * @return ProyectoBAE
	 */
	public function setExpediente( \MesaEntradaBundle\Entity\Expediente $expediente = null ) {
		$this->expediente = $expediente;

		return $this;
	}

	/**
	 * Get expediente
	 *
	 * @return \MesaEntradaBundle\Entity\Expediente
	 */
	public function getExpediente() {
		return $this->expediente;
	}

	/**
	 * Set boletinAsuntoEntrado
	 *
	 * @param \AppBundle\Entity\BoletinAsuntoEntrado $boletinAsuntoEntrado
	 *
	 * @return ProyectoBAE
	 */
	public function setBoletinAsuntoEntrado( \AppBundle\Entity\BoletinAsuntoEntrado $boletinAsuntoEntrado = null ) {
		$this->boletinAsuntoEntrado = $boletinAsuntoEntrado;

		return $this;
	}

	/**
	 * Get boletinAsuntoEntrado
	 *
	 * @return \AppBundle\Entity\BoletinAsuntoEntrado
	 */
	public function getBoletinAsuntoEntrado() {
		return $this->boletinAsuntoEntrado;
	}

	/**
	 * Set creadoPor
	 *
	 * @param \UsuariosBundle\Entity\Usuario $creadoPor
	 *
	 * @return ProyectoBAE
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
	 * @return ProyectoBAE
	 */
	public function setActualizadoPor( \UsuariosBundle\Entity\Usuario $actualizadoPor = null ) {
		$this->actualizadoPor = $actualizadoPor;

		return $this;
	}

    /**
     * Set esInformeDem
     *
     * @param boolean $esInformeDem
     *
     * @return ProyectoBAE
     */
    public function setEsInformeDem($esInformeDem)
    {
        $this->esInformeDem = $esInformeDem;

        return $this;
    }

    /**
     * Get esInformeDem
     *
     * @return boolean
     */
    public function getEsInformeDem()
    {
        return $this->esInformeDem;
    }

    /**
     * Set extracto
     *
     * @param string $extracto
     *
     * @return ProyectoBAE
     */
    public function setExtracto($extracto)
    {
        $this->extracto = $extracto;

        return $this;
    }

    /**
     * Get extracto
     *
     * @return string
     */
    public function getExtracto()
    {
        return $this->extracto;
    }
    /**
     * Constructor
     */
    public function __construct()
    {
        $this->giros = new \Doctrine\Common\Collections\ArrayCollection();
    }

    /**
     * Add giro
     *
     * @param \MesaEntradaBundle\Entity\Giro $giro
     *
     * @return ProyectoBAE
     */
    public function addGiro(\MesaEntradaBundle\Entity\Giro $giro)
    {
        $giro->setProyectoBae( $this );

        $this->giros->add( $giro );

        return $this;
    }

    /**
     * Remove giro
     *
     * @param \MesaEntradaBundle\Entity\Giro $giro
     */
    public function removeGiro(\MesaEntradaBundle\Entity\Giro $giro)
    {
        $this->giros->removeElement($giro);
    }

    /**
     * Get giros
     *
     * @return \Doctrine\Common\Collections\Collection
     */
    public function getGiros()
    {
        return $this->giros;
    }

    public function getGirosOrdenados()
    {
        $iterator = $this->getGiros()->getIterator();

        $iterator->uasort(function (Giro $a, Giro $b) {
            if ($a->getCabecera()) {
                return -1;
            } elseif ($b->getCabecera()) {
                return 1;
            } else {
                return ($a->getOrden() < $b->getOrden()) ? -1 : 1;
            }
        });

        return new \Doctrine\Common\Collections\ArrayCollection(iterator_to_array($iterator));
    }

    /**
     * Set incorporadoEnSesion
     *
     * @param boolean $incorporadoEnSesion
     *
     * @return ProyectoBAE
     */
    public function setIncorporadoEnSesion($incorporadoEnSesion)
    {
        $this->incorporadoEnSesion = $incorporadoEnSesion;

        return $this;
    }

    /**
     * Get incorporadoEnSesion
     *
     * @return boolean
     */
    public function getIncorporadoEnSesion()
    {
        return $this->incorporadoEnSesion;
    }

	/**
	 * @return mixed
	 */
	public function getTratamientoSobretabla() {
		return $this->tratamientoSobretabla;
	}

	/**
	 * @param mixed $tratamientoSobretabla
	 */
	public function setTratamientoSobretabla( $tratamientoSobretabla ) {
		$this->tratamientoSobretabla = $tratamientoSobretabla;
	}
}
