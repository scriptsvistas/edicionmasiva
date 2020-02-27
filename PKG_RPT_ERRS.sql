--------------------------------------------------------
-- Archivo creado  - jueves-febrero-27-2020   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package PKG_RPT_ERRS
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "VISUAL"."PKG_RPT_ERRS" is
 
  -- Author  : SCASTILLO 
  -- Created : 04/02/2015
   
     
  TYPE Tr_Columnas_ValError IS RECORD(
    tipoerror           NVARCHAR2(15),
    descerror           NVARCHAR2(150),
    alimentadorid       NVARCHAR2(10),
    descalimentador     NVARCHAR2(20),
    elemento            VARCHAR2(30) ,
    objectid            INTEGER ,
    datoadicional1      NVARCHAR2(100),
    datoadicional2      NVARCHAR2(100),
    fechavalidacion     date,
    usr_registro        NVARCHAR2(50),
    fec_registro        date,
    usr_mod_registro    NVARCHAR2(50),
    fec_mod_registro    NVARCHAR2(50)
    ); 
 
  TYPE Gr_ValidaError IS TABLE OF Tr_Columnas_ValError INDEX BY BINARY_INTEGER;

  --Definición de Variables
  Gv_DescERROR      VALIDAERROR.DESCERROR%TYPE;
  Gv_Error          VARCHAR2(2000);


  --Definición de Procedimientos
  PROCEDURE P_V_FechaProceso(Fecha  OUT date);

  PROCEDURE P_V_DatosErrores (Pr_Seleccionados OUT PKG_RPT_ERRS.Gr_ValidaError,
                              Pv_Error         OUT VARCHAR2);

  PROCEDURE P_EliminaDatosTablaError;

  PROCEDURE P_Principal;
  
  PROCEDURE P_I_InicioProceso; 
  
  PROCEDURE P_I_FinProceso;
 
  --Validaciones aplicables a varios elementos
  PROCEDURE P_V_FaseConexionySubtipo;

  PROCEDURE P_V_FaseConexionFeatureyTrafo;

  PROCEDURE P_V_FeederInfo;

  PROCEDURE P_V_BTconParentCSDisyuntor;

  --Validaciones especificas para cada elemento
  --Seccionador Fusible
  PROCEDURE P_V_SF_FaseEstrucVoltCorr;

  PROCEDURE P_V_SF_FaseConexionyUnidadFusi;

  PROCEDURE P_V_SF_CapacidadConValorNULL;

  --Puesto Corrector Factor Potencia(Capacitor)
  PROCEDURE P_V_CA_FaseConexionyUnidad;

  PROCEDURE P_V_CA_PotenciaUnidadyPuesto;

  --Tramos Distribución Aereo y Subterráneo
  PROCEDURE P_V_TM_ConductorConfySecFase;

  PROCEDURE P_V_TM_ConductorFaseyVoltaje;
 
  --Puesto de Protección Dinámico
  PROCEDURE P_V_PD_CorrienteFaseyVoltaje;

  --Puesto Transformador
  PROCEDURE P_V_PT_NTapsyConfLadoBajaMedia;

  PROCEDURE P_V_PT_UnidadySubtipo;

  PROCEDURE P_V_PT_FaseConexPuestoyUnidad;

  PROCEDURE P_V_PT_FaseConexionBancoTR;

  PROCEDURE P_V_PT_PotenciaUnidadyPuesto;

  --Conexion Consumidor
  PROCEDURE P_V_CC_CodigoClienteRepetido;
  
  PROCEDURE P_V_CC_CodigoUnicoRepetido;

  PROCEDURE P_V_CC_ConCodClienteSinMedidor;

  PROCEDURE P_V_CC_SinCodClienteConMedidor;

  PROCEDURE P_V_CC_SinCodClienteSinMedidor;
  
  PROCEDURE P_V_CC_SinCodUnicoSinNovedad;
 
end PKG_RPT_ERRS;

/
