<?php

namespace MesaEntradaBundle\Entity;

use Doctrine\ORM\Mapping as ORM;
use UtilBundle\Entity\Base\BaseClass;

/**
 * Giro
 *
 * @ORM\Table(name="giro")
 * @ORM\Entity(repositoryClass="MesaEntradaBundle\Repository\GiroRepository")
 */
class Giro extends BaseClass {
	/**
	 * @var int
	 *
	 * @ORM\Column(name="id", type="integer")
	 * @ORM\Id
	 * @ORM\GeneratedValue(strategy="AUTO")
	 */
	private $id;

	/**
	 * @var bool
	 *
	 * @ORM\Column(name="cabecera", type="boolean", nullable=true)
	 */
	private $cabecera;

	/**
	 * @var \DateTime
	 *
	 * @ORM\Column(name="fecha_giro", type="date")
	 */
	private $fechaGiro;

	/**
	 * @var
	 *
	 * @ORM\ManyToOne(targetEntity="AppBundle\Entity\Comision")
	 * @ORM\JoinColumn(name="comision_origen_id", referencedColumnName="id")
	 */
	private $comisionOrigen;

	/**
	 * @var
	 *
	 * @ORM\ManyToOne(targetEntity="AppBundle\Entity\Comision")
	 * @ORM\JoinColumn(name="comision_destino_id", referencedColumnName="id")
	 */
	private $comisionDestino;

	/**
	 * @var
	 *
	 * @ORM\ManyToOne(targetEntity="MesaEntradaBundle\Entity\Expediente", inversedBy="giros")
	 * @ORM\JoinColumn(name="expediente_id", referencedColumnName="id")
	 */
	private $expediente;

	/**
	 * @var bool
	 *
	 * @ORM\Column(name="archivado", type="boolean", nullable=true)
	 */
	private $archivado;

	/**
	 * @var string
	 *
	 * @ORM\Column(name="texto", type="text", nullable=true)
	 */
	private $texto;

	/**
	 * @var string
	 *
	 * @ORM\Column(name="orden", type="integer", nullable=true)
	 */
	private $orden;

    /**
     * @var
     *
     * @ORM\ManyToOne(targetEntity="AppBundle\Entity\ProyectoBAE", inversedBy="giros")
     * @ORM\JoinColumn(name="proyecto_bae_id", referencedColumnName="id")
     */
    private $proyectoBae;

    /**
     * Get id
     *
     * @return integer
     */
    public function getId()
    {
        return $this->id;
    }

    /**
     * Set cabecera
     *
     * @param boolean $cabecera
     *
     * @return Giro
     */
    public function setCabecera($cabecera)
    {
        $this->cabecera = $cabecera;

        return $this;
    }

    /**
     * Get cabecera
     *
     * @return boolean
     */
    public function getCabecera()
    {
        return $this->cabecera;
    }

    /**
     * Set fechaGiro
     *
     * @param \DateTime $fechaGiro
     *
     * @return Giro
     */
    public function setFechaGiro($fechaGiro)
    {
        $this->fechaGiro = $fechaGiro;

        return $this;
    }

    /**
     * Get fechaGiro
     *
     * @return \DateTime
     */
    public function getFechaGiro()
    {
        return $this->fechaGiro;
    }

    /**
     * Set fechaCreacion
     *
     * @param \DateTime $fechaCreacion
     *
     * @return Giro
     */
    public function setFechaCreacion($fechaCreacion)
    {
        $this->fechaCreacion = $fechaCreacion;

        return $this;
    }

    /**
     * Set fechaActualizacion
     *
     * @param \DateTime $fechaActualizacion
     *
     * @return Giro
     */
    public function setFechaActualizacion($fechaActualizacion)
    {
        $this->fechaActualizacion = $fechaActualizacion;

        return $this;
    }

    /**
     * Set comisionOrigen
     *
     * @param \AppBundle\Entity\Comision $comisionOrigen
     *
     * @return Giro
     */
    public function setComisionOrigen(\AppBundle\Entity\Comision $comisionOrigen = null)
    {
        $this->comisionOrigen = $comisionOrigen;

        return $this;
    }

    /**
     * Get comisionOrigen
     *
     * @return \AppBundle\Entity\Comision
     */
    public function getComisionOrigen()
    {
        return $this->comisionOrigen;
    }

    /**
     * Set comisionDestino
     *
     * @param \AppBundle\Entity\Comision $comisionDestino
     *
     * @return Giro
     */
    public function setComisionDestino(\AppBundle\Entity\Comision $comisionDestino = null)
    {
        $this->comisionDestino = $comisionDestino;

        return $this;
    }

    /**
     * Get comisionDestino
     *
     * @return \AppBundle\Entity\Comision
     */
    public function getComisionDestino()
    {
        return $this->comisionDestino;
    }

    /**
     * Set expediente
     *
     * @param \MesaEntradaBundle\Entity\Expediente $expediente
     *
     * @return Giro
     */
    public function setExpediente(\MesaEntradaBundle\Entity\Expediente $expediente = null)
    {
        $this->expediente = $expediente;

        return $this;
    }

    /**
     * Get expediente
     *
     * @return \MesaEntradaBundle\Entity\Expediente
     */
    public function getExpediente()
    {
        return $this->expediente;
    }

    /**
     * Set creadoPor
     *
     * @param \UsuariosBundle\Entity\Usuario $creadoPor
     *
     * @return Giro
     */
    public function setCreadoPor(\UsuariosBundle\Entity\Usuario $creadoPor = null)
    {
        $this->creadoPor = $creadoPor;

        return $this;
    }

    /**
     * Set actualizadoPor
     *
     * @param \UsuariosBundle\Entity\Usuario $actualizadoPor
     *
     * @return Giro
     */
    public function setActualizadoPor(\UsuariosBundle\Entity\Usuario $actualizadoPor = null)
    {
        $this->actualizadoPor = $actualizadoPor;

        return $this;
    }

    /**
     * Set archivado
     *
     * @param boolean $archivado
     *
     * @return Giro
     */
    public function setArchivado($archivado)
    {
        $this->archivado = $archivado;

        return $this;
    }

    /**
     * Get archivado
     *
     * @return boolean
     */
    public function getArchivado()
    {
        return $this->archivado;
    }

    /**
     * Set texto
     *
     * @param string $texto
     *
     * @return Giro
     */
    public function setTexto($texto)
    {
        $this->texto = $texto;

        return $this;
    }

    /**
     * Get texto
     *
     * @return string
     */
    public function getTexto()
    {
        return $this->texto;
    }

    /**
     * Set orden
     *
     * @param integer $orden
     *
     * @return Giro
     */
    public function setOrden($orden)
    {
        $this->orden = $orden;

        return $this;
    }

    /**
     * Get orden
     *
     * @return integer
     */
    public function getOrden()
    {
        return $this->orden;
    }

    /**
     * Set proyectoBae
     *
     * @param \AppBundle\Entity\ProyectoBAE $proyectoBae
     *
     * @return Giro
     */
    public function setProyectoBae(\AppBundle\Entity\ProyectoBAE $proyectoBae = null)
    {
        $this->proyectoBae = $proyectoBae;

        return $this;
    }

    /**
     * Get proyectoBae
     *
     * @return \AppBundle\Entity\ProyectoBAE
     */
    public function getProyectoBae()
    {
        return $this->proyectoBae;
    }
}
