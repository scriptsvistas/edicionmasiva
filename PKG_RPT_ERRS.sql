CREATE OR REPLACE PACKAGE                   PKG_RPT_ERRS is
 
  -- Author  : SCASTILLO 
  -- Created : 04/02/2015
  -- Subir 2  
     
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


CREATE OR REPLACE PACKAGE BODY                            PKG_RPT_ERRS is
/******************************************************************************************/
     
                                         -- 1 --
                                              
/******************************************************************************************/
  PROCEDURE P_V_FechaProceso (Fecha  OUT date) IS 
/******************************************************************************************/
  -----
  BEGIN  
  -----
    SELECT DISTINCT ve.fechavalidacion
               into Fecha
    FROM VALIDAERROR ve;
  ---  
  END P_V_FechaProceso; 
  ---
/******************************************************************************************/
  
                                        -- 2 --
  
/******************************************************************************************/
  PROCEDURE P_V_DatosErrores (Pr_Seleccionados  OUT PKG_RPT_ERRS.Gr_ValidaError,
                              Pv_Error          OUT VARCHAR2) IS
/******************************************************************************************/
  CURSOR C_ValidaERROR IS
                SELECT ve.tipoerror,
                       ve.descerror,
                       ve.alimentadorid,
                       ve.descalimentador,
                       ve.elemento,
                       ve.objectid,
                       ve.datoadicional1,
                       ve.datoadicional2,
                       ve.fechavalidacion,
                       ve.usr_registro,       -- usuario registro
                       ve.fec_registro,       -- fecha registro
                       ve.usr_mod_registro,   -- usuario modificacion registro
                       ve.fec_mod_registro    -- fecha modificacion registro
                FROM VALIDAERROR ve;

  Ln_Total_Registros   NUMBER;
  -----
  BEGIN
  -----
    OPEN C_ValidaERROR;
    FETCH C_ValidaERROR BULK COLLECT
      INTO Pr_Seleccionados;
    Ln_Total_Registros := C_ValidaERROR%ROWCOUNT;
    CLOSE C_ValidaERROR;

    IF (Ln_Total_Registros = 0) THEN
      Pv_Error := 'NO EXISTE REGISTROS DE ERROR EN ArcGIS';
    END IF;

    EXCEPTION
      WHEN OTHERS THEN
        Pv_Error := 'Error: ' || SQLERRM;
  ---
  END P_V_DatosErrores;
  ---
/******************************************************************************************/

                                        -- 3 --

/******************************************************************************************/
  PROCEDURE P_EliminaDatosTablaError IS
/******************************************************************************************/
  -----
  BEGIN
  -----
    DELETE FROM VALIDAERROR;
    COMMIT;
  ---
  END P_EliminaDatosTablaError;
  ---
/******************************************************************************************/

                                        -- 4 --

/******************************************************************************************/
  PROCEDURE P_Principal IS
/******************************************************************************************/

  CURSOR C_States is
            SELECT COUNT(*) 
            FROM sde.states;

  Ln_Registros   NUMBER;
  Lv_Error       VARCHAR2(100);
  Le_Error       EXCEPTION;
  n_regs_sesion  INTEGER;
  
  -----
  BEGIN
  -----
  
    dbms_output.enable(1000);
    dbms_output.put_line ('Eliminando Datos tabla de errores');
    
    -- Eliminar datos procesados anteriomente
    P_EliminaDatosTablaError;

    -- Validación de proceso de compresión
    n_regs_sesion := 1;
    n_regs_sesion := FUNC_VERIFICA_ESTADO_0('SIGELEC');
    /*
    OPEN C_States;
    FETCH C_States
     INTO Ln_Registros;
    CLOSE C_States;
    */
    IF (Ln_Registros <> 0) THEN
       Lv_Error := 'No se ha realizado el proceso de Compresión';
       RAISE Le_Error;
    END IF;
    
    -- Inicio del proceso
    P_I_InicioProceso; 
  
    -- Procesamiento
    -- Validaciones aplicables a varios elementos
    dbms_output.put_line ('Validaciones aplicadas a varios elementos');
    P_V_FaseConexionySubtipo;
    P_V_FaseConexionFeatureyTrafo;
    P_V_FeederInfo;
    P_V_BTconParentCSDisyuntor;

    -- Validaciones especificas para cada elemento
    -- Seccionador Fusible
    dbms_output.put_line ('Validaciones aplicadas a Seccionador Fusible');
    P_V_SF_FaseEstrucVoltCorr;
    P_V_SF_FaseConexionyUnidadFusi;
    P_V_SF_CapacidadConValorNULL;

    -- Puesto Corrector Factor Potencia(Capacitor)
    dbms_output.put_line ('Validaciones aplicadas a Capacitor');
    P_V_CA_FaseConexionyUnidad;
    P_V_CA_PotenciaUnidadyPuesto;

    -- Tramos Distribución Aereo y Subterráneo
    dbms_output.put_line ('Validaciones aplicadas a Tramo Media');
    P_V_TM_ConductorConfySecFase;
    P_V_TM_ConductorFaseyVoltaje;

    -- Puesto de Protección Dinámico
    dbms_output.put_line ('Validaciones aplicadas a Proteccion Dinamico');
    P_V_PD_CorrienteFaseyVoltaje;

    -- Puesto Transformador
    dbms_output.put_line ('Validaciones aplicadas a Transformador');
    P_V_PT_NTapsyConfLadoBajaMedia;
    P_V_PT_UnidadySubtipo;
    P_V_PT_FaseConexPuestoyUnidad;
    P_V_PT_FaseConexionBancoTR;
    P_V_PT_PotenciaUnidadyPuesto;
 
    -- Conexion Consumidor
    dbms_output.put_line ('Validaciones aplicadas a ConexionConsumidor');
    P_V_CC_CodigoClienteRepetido;
    P_V_CC_ConCodClienteSinMedidor;
    P_V_CC_SinCodClienteConMedidor;
    -- P_V_CC_SinCodClienteSinMedidor;

    -- Fin de Proceso
    P_I_FinProceso;
    
    EXCEPTION
    
      WHEN Le_Error THEN
        ROLLBACK;
          Gv_Error := Lv_Error;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: Principal', NULL, TRUNC(sysdate), NULL,NULL,NULL,NULL);

      COMMIT;

      WHEN OTHERS THEN
        ROLLBACK;
          Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: Principal', NULL, TRUNC(sysdate), NULL,NULL,NULL,NULL);

      COMMIT;
  ---
  END P_Principal;
  ---
/******************************************************************************************/

                                        -- 5 --

/******************************************************************************************/
  PROCEDURE P_I_InicioProceso IS    
/******************************************************************************************/
  -----
  BEGIN
  ----- 
    -- Registro de cuando inició el proceso 
    Gv_DescERROR:= 'Inicio de Proceso';
    
    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro) 
    VALUES ('INFO', Gv_DescERROR, NULL, NULL, ' ', 0, to_char(sysdate, 'dd/mm/yyyy hh:mm:ss'), NULL, TRUNC(sysdate), NULL,NULL,NULL,NULL);          

    COMMIT;
    
    EXCEPTION
    
      WHEN OTHERS THEN
        ROLLBACK;
          Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro) 
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_I_InicioProceso', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
      
      COMMIT;
  ---
  END P_I_InicioProceso; 
  ---
/******************************************************************************************/

                                        -- 6 --

/******************************************************************************************/
  PROCEDURE P_I_FinProceso IS    
/******************************************************************************************/
  -----
  BEGIN    
  -----
    -- Registro de cuando finalizó el proceso 
    
    Gv_DescERROR:= 'Fin de Proceso';
    
    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro) 
    VALUES ('INFO', Gv_DescERROR, NULL, NULL, ' ', 0, to_char(sysdate, 'dd/mm/yyyy hh:mm:ss'), NULL, TRUNC(sysdate), NULL,NULL,NULL,NULL);          

    COMMIT;
    
    EXCEPTION
    
      WHEN OTHERS THEN
        ROLLBACK;
          Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro) 
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_I_FinProceso', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
      
      COMMIT;
  ---   
  END P_I_FinProceso; 
  ---
/******************************************************************************************/

                                        -- 7 --

-- Validaciones aplicables a varios elementos
/******************************************************************************************/
  PROCEDURE P_V_FaseConexionySubtipo IS
/******************************************************************************************/

  CURSOR C_Datos IS
        SELECT  F.alimentadorid,
                F.DescAlimentador,
                F.Elemento, 
                F.objectid,
                'Cantidad por Fase Conexion: '  || F.CantFase datoadicional1,
                'Cantidad por Subtipo: '        || F.CantFasexSubtipo datoadicional2,
                TRUNC(sysdate)  fechavalidacion,
                F.usr_registro,
                F.fec_registro,
                F.usr_mod_registro,
                F.fec_mod_registro
        FROM (
        
          SELECT 'MTA' Elemento,
                  get_descripcion_alimentador(tda.alimentadorid) DescAlimentador,
                  tda.objectid,
                  tda.alimentadorid,
                  CASE
                    WHEN tda.FaseConexion IN (1,2,4) THEN 1 -- Monofásico
                    WHEN tda.FaseConexion IN (3,5,6) THEN 2 -- Bifásico
                    WHEN tda.FaseConexion = 7 then 3        -- Trifásico
                    ELSE 0
                  END CantFase,
                  tda.faseconexion,
                  CASE
                    WHEN tda.subtipo IN (1,4) THEN 1        -- Tramo y Bajante Monofásico
                    WHEN tda.subtipo IN (2,5) THEN 2        -- Tramo y Bajante Bifásico
                    WHEN tda.subtipo IN (3,6) THEN 3        -- Tramo y Bajante Trifásico
                  END CantFasexSubtipo,
                  tda.usuarioregistro usr_registro,
                  tda.fecharegistro   fec_registro,
                  tda.usuariomodificacionregistro usr_mod_registro,
                  tda.fechamodificacionregistro fec_mod_registro
          FROM tramodistribucionaereo tda
          
          UNION ALL
          
          SELECT 'MTS',
                  get_descripcion_alimentador(tds.alimentadorid) DescAlimentador,
                  tds.objectid,
                  tds.alimentadorid,
                  CASE
                    WHEN tds.FaseConexion in (1,2,4) then 1 -- Monofásico
                    WHEN tds.FaseConexion in (3,5,6) then 2 -- Bifásico
                    WHEN tds.FaseConexion = 7 then 3        -- Trifásico
                    ELSE 0
                  END CantFase,
                  tds.faseconexion,
                  CASE
                    WHEN tds.subtipo in (1,4) then 1        -- Tramo y Bajante Monofásico
                    WHEN tds.subtipo in (2,5) then 2        -- Tramo y Bajante Bifásico
                    WHEN tds.subtipo in (3,6) then 3        -- Tramo y Bajante Trifásico
                  END CantFasexSubtipo,
                  tds.usuarioregistro usr_registro,
                  tds.fecharegistro   fec_registro,
                  tds.usuariomodificacionregistro usr_mod_registro,
                  tds.fechamodificacionregistro fec_mod_registro
          FROM tramodistribucionsubterraneo tds
        
          UNION ALL
      
          SELECT 'BTA' Elemento,
                  get_descripcion_alimentador(tba.alimentadorid) DescAlimentador,
                  tba.objectid,
                  tba.alimentadorid,
                  CASE
                    WHEN tba.FaseConexion in (1,2,4) then 1 -- Monofásico
                    WHEN tba.FaseConexion in (3,5,6) then 2 -- Bifásico
                    WHEN tba.FaseConexion = 7 then 3        -- Trifásico
                    ELSE 0
                  END CantFase,
                  tba.faseconexion,
                  CASE
                    WHEN tba.subtipo in (1,4,7) then 1      -- Tramo, Bajante y Acometida Monofásico
                    WHEN tba.subtipo in (2,5,8) then 2      -- Tramo, Bajante y Acometida Bifásico
                    WHEN tba.subtipo in (3,6,9) then 3      -- Tramo, Bajante y Acometida Trifásico
                  END CantFasexSubtipo,
                  tba.usuarioregistro             usr_registro,
                  tba.fecharegistro               fec_registro,
                  tba.usuariomodificacionregistro usr_mod_registro,
                  tba.fechamodificacionregistro   fec_mod_registro

          FROM tramobajatensionaereo tba
      
          UNION ALL
      
          SELECT 'BTS',
                  get_descripcion_alimentador(tbs.alimentadorid) DescAlimentador,
                  tbs.objectid,
                  tbs.alimentadorid,
                  CASE
                    WHEN tbs.FaseConexion in (1,2,4) then 1 -- Monofásico
                    WHEN tbs.FaseConexion in (3,5,6) then 2 -- Bifásico
                    WHEN tbs.FaseConexion = 7 then 3        -- Trifásico
                    ELSE 0
                  END CantFase,
                  tbs.faseconexion,
                  CASE
                    WHEN tbs.subtipo in (1,4,7) then 1      -- Tramo, Bajante y Acometida Monofásico
                    WHEN tbs.subtipo in (2,5,8) then 2      -- Tramo, Bajante y Acometida Bifásico
                    WHEN tbs.subtipo in (3,6,9) then 3      -- Tramo, Bajante y Acometida Trifásico
                    END CantFasexSubtipo,
                  tbs.usuarioregistro             usr_registro,
                  tbs.fecharegistro               fec_registro,
                  tbs.usuariomodificacionregistro usr_mod_registro,
                  tbs.fechamodificacionregistro   fec_mod_registro
          FROM tramobajatensionsubterraneo tbs
          
          UNION ALL
          
          SELECT 'Transformadores',
                  get_descripcion_alimentador(pt.alimentadorid) DescAlimentador,
                  pt.objectid,
                  pt.alimentadorid,
                  CASE
                    WHEN pt.FaseConexion in (1,2,4) then 1  --Monofásico
                    WHEN pt.FaseConexion in (3,5,6) then 2  --Bifásico
                    WHEN pt.FaseConexion = 7 then 3         --Trifásico
                    ELSE 0
                  END CantFase,
                  pt.faseconexion,
                  CASE
                    WHEN pt.subtipo in (1,2,3,4) then 1       --Monofásicos
                    WHEN pt.subtipo in (13,9,10) then 2       --Bifásicos y Banco de 2 Transformadores
                    WHEN pt.subtipo in (5,6,7,8,11,12) then 3 --Trifásicos y Banco de 3 Transformadores
                  END CantFasexSubtipo,
                  pt.usuarioregistro              usr_registro,
                  pt.fecharegistro                fec_registro,
                  pt.usuariomodificacionregistro  usr_mod_registro,
                  pt.fechamodificacionregistro    fec_mod_registro

          FROM puestotransfdistribucion pt
    )F
    WHERE F.CantFase <> F.CantFasexSubtipo;

    Lc_Datos  C_Datos%ROWTYPE;

  -----
  BEGIN
  -----
  
    -- Validacion de la FASE DE CONEXION con el SUBTIPO,
    -- el subtipo seleccionado debe ir de acuerdo a la cantidad de fases que indica la Fase de conexion

    Gv_DescERROR:= 'Fase de conexion con el Subtipo asignado';

    FOR Lc_Datos IN C_Datos LOOP
      BEGIN
        INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
        VALUES('ADMS', Gv_DescERROR, Lc_Datos.alimentadorid, Lc_Datos.DescAlimentador, Lc_Datos.Elemento, Lc_Datos.objectid, Lc_Datos.datoadicional1, Lc_Datos.datoadicional2, Lc_Datos.fechavalidacion, Lc_Datos.usr_registro, Lc_Datos.fec_registro, Lc_Datos.usr_mod_registro, Lc_Datos.fec_mod_registro);
      END;
    END LOOP;

    COMMIT;

    EXCEPTION
  
     WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_FaseConexionySubtipo', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);

      COMMIT;
  ---    
  END P_V_FaseConexionySubtipo;
  ---
/******************************************************************************************/

                                        -- 7 --                                        
/******************************************************************************************/
  PROCEDURE P_V_FaseConexionFeatureyTrafo IS
/******************************************************************************************/
   CURSOR C_Datos IS
        SELECT  F.alimentadorid,
                F.DescAlimentador,
                F.Elemento,
                F.objectid,
                F.FaseConexionClteDesc datoadicional1,
                F.FaseConexionTrafo datoadicional2,
                TRUNC(sysdate) fechavalidacion,
                F.usr_registro,
                F.fec_registro,
                F.usr_mod_registro,
                F.fec_mod_registro
        FROM (
              SELECT  pt.alimentadorid,
                      GET_DESCRIPCION_ALIMENTADOR(pt.alimentadorid) DescAlimentador,
                      pt.objectid PuestoTransDistObjectID,
                      D.Elemento,
                      D.ObjectID,
                      'FaseCliente=' || FaseConexionClte FaseConexionClteDesc,
                      'FaseTrafo='   ||
                      CASE 
                        WHEN pt.FaseConexion = 4 THEN 'A'
                        WHEN pt.FaseConexion = 2 THEN 'B'
                        WHEN pt.FaseConexion = 1 THEN 'C'
                        WHEN pt.FaseConexion = 5 THEN 'AC'
                        WHEN pt.FaseConexion = 6 THEN 'AB'
                        WHEN pt.FaseConexion = 3 THEN 'BC'
                        WHEN pt.FaseConexion = 7 THEN 'ABC'
                      END FaseConexionTrafo,
                      D.usuarioregistro              usr_registro,
                      D.fecharegistro                fec_registro,
                      D.usuariomodificacionregistro  usr_mod_registro,
                      D.fechamodificacionregistro    fec_mod_registro
              FROM puestotransfdistribucion pt
              LEFT OUTER JOIN
              (
                SELECT  a.ParentCircuitSourceguid,
                        'Punto Carga' Elemento,
                        a.objectid,
                        a.FaseConexion,
                        CASE 
                          WHEN a.FaseConexion = 4 THEN 'A'
                          WHEN a.FaseConexion = 2 THEN 'B'
                          WHEN a.FaseConexion = 1 THEN 'C'
                          WHEN a.FaseConexion = 5 THEN 'AC'
                          WHEN a.FaseConexion = 6 THEN 'AB'
                          WHEN a.FaseConexion = 3 THEN 'BC'
                          WHEN a.FaseConexion = 7 THEN 'ABC'
                        END FaseConexionClte,
                        a.usuarioregistro,
                        a.fecharegistro,
                        a.usuariomodificacionregistro,
                        a.fechamodificacionregistro
                  FROM PuntoCarga a
                  INNER JOIN ConexionConsumidor b
                  ON a.globalid = b.puntocargaglobalid
                  
                  UNION ALL
                  
                  SELECT a.ParentCircuitSourceguid,
                         'Luminaria',
                         a.objectid,
                         a.FaseConexion,
                         CASE 
                            WHEN a.FaseConexion = 4 THEN 'A'
                            WHEN a.FaseConexion = 2 THEN 'B'
                            WHEN a.FaseConexion = 1 THEN 'C'
                            WHEN a.FaseConexion = 5 THEN 'AC'
                            WHEN a.FaseConexion = 6 THEN 'AB'
                            WHEN a.FaseConexion = 3 THEN 'BC'
                            WHEN a.FaseConexion = 7 THEN 'ABC'
                          END FaseConexionClte,
                          a.usuarioregistro,
                          a.fecharegistro,
                          a.usuariomodificacionregistro,
                          a.fechamodificacionregistro
                  FROM Luminaria a
           ) D
           ON pt.circuitsourceguid  = D.parentcircuitsourceguid
           WHERE Length(TRIM(translate(D.FaseConexionClte,decode(pt.FaseConexion,4,'A',2,'B',1,'C',6,'AB',5,'AC',7,'ABC',3,'BC'), '   '))) > 0
      )F;

  Lc_Datos  C_Datos%ROWTYPE;

  BEGIN
    -- Fase de conexion diferente a la fase del Transformador asociado (Punto Carga  y Luminaria)

    Gv_DescERROR:= 'Fase de Conexión del Feature con el Trafo relacionado';

    FOR Lc_Datos IN C_Datos LOOP
      BEGIN
        INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
        VALUES('ADMS', Gv_DescERROR, Lc_Datos.alimentadorid, Lc_Datos.DescAlimentador, Lc_Datos.Elemento, Lc_Datos.objectid, Lc_Datos.datoadicional1, Lc_Datos.datoadicional2, Lc_Datos.fechavalidacion, Lc_Datos.usr_registro, Lc_Datos.fec_registro, Lc_Datos.usr_mod_registro, Lc_Datos.fec_mod_registro);
      END;
    END LOOP;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_FaseConexionFeatureyTrafo', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);

      COMMIT;
  END P_V_FaseConexionFeatureyTrafo;
/******************************************************************************************/

                                        -- 8 --

/******************************************************************************************/
  PROCEDURE P_V_FeederInfo IS
/******************************************************************************************/

  CURSOR C_Datos IS
        SELECT  F.alimentadorid,
                F.DescAlimentador,
                F.Elemento,
                F.objectid,
                F.DescError datoadicional1,
                NULL datoadicional2,
                TRUNC(sysdate) fechavalidacion,
                F.usr_registro,
                F.fec_registro,
                F.usr_mod_registro,
                F.fec_mod_registro

        FROM (
              SELECT 'Tramo Baja Aereo' Elemento,
                      b.objectid,
                      b.alimentadorid,
                      GET_DESCRIPCION_ALIMENTADOR(b.alimentadorid) DescAlimentador,
                      f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo) DescError,
                      b.usuarioregistro usr_registro,
                      b.fecharegistro   fec_registro,
                      b.usuariomodificacionregistro usr_mod_registro,
                      b.fechamodificacionregistro fec_mod_registro
              FROM tramobajatensionaereo b
              WHERE faseconexion is not null AND 
                    f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo) <> 'OK'
                
              UNION ALL
          
              SELECT 'Tramo Baja Subterraneo' Elemento,
                      b.objectid,
                      b.alimentadorid,
                      GET_DESCRIPCION_ALIMENTADOR(b.alimentadorid) DescAlimentador,
                      f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo),
                      b.usuarioregistro usr_registro,
                      b.fecharegistro   fec_registro,
                      b.usuariomodificacionregistro usr_mod_registro,
                      b.fechamodificacionregistro fec_mod_registro
              FROM tramobajatensionsubterraneo b
              WHERE faseconexion is not null AND 
                    f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo) <> 'OK'
          
              UNION ALL
          
              SELECT 'Tramo Distribución Aereo' Elemento,
                      b.objectid,
                      b.alimentadorid,
                      GET_DESCRIPCION_ALIMENTADOR(b.alimentadorid) DescAlimentador,
                      f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo),
                      b.usuarioregistro usr_registro,
                      b.fecharegistro   fec_registro,
                      b.usuariomodificacionregistro usr_mod_registro,
                      b.fechamodificacionregistro fec_mod_registro
              FROM tramodistribucionaereo b
              WHERE faseconexion is not null AND 
                    f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo) <> 'OK'
                
              UNION ALL
          
              SELECT 'Tramo Distribución Subterraneo' Elemento,
                      b.objectid,
                      b.alimentadorid,
                      GET_DESCRIPCION_ALIMENTADOR(b.alimentadorid) DescAlimentador,
                      f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo),
                      b.usuarioregistro usr_registro,
                      b.fecharegistro   fec_registro,
                      b.usuariomodificacionregistro usr_mod_registro,
                      b.fechamodificacionregistro fec_mod_registro
              FROM tramodistribucionsubterraneo b
              WHERE faseconexion is not null AND 
                    f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo) <> 'OK'
                
              UNION ALL
          
              SELECT 'Luminaria' Elemento,
                      b.objectid,
                      b.alimentadorid,
                      GET_DESCRIPCION_ALIMENTADOR(b.alimentadorid) DescAlimentador,
                      f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo),
                      b.usuarioregistro usr_registro,
                      b.fecharegistro   fec_registro,
                      b.usuariomodificacionregistro usr_mod_registro,
                      b.fechamodificacionregistro fec_mod_registro
              FROM luminaria b
              WHERE faseconexion is not null AND 
                    f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo) <> 'OK'
                
              UNION ALL
              
              SELECT 'Transformador' Elemento,
                      b.objectid,
                      b.alimentadorid,
                      GET_DESCRIPCION_ALIMENTADOR(b.alimentadorid) DescAlimentador,
                      f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo),
                      b.usuarioregistro usr_registro,
                      b.fecharegistro   fec_registro,
                      b.usuariomodificacionregistro usr_mod_registro,
                      b.fechamodificacionregistro fec_mod_registro
              FROM puestotransfdistribucion b
              WHERE faseconexion is not null AND 
                    f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo) <> 'OK'
                
              UNION ALL
          
              SELECT 'Seccionador Fusible' Elemento,
                      b.objectid,
                      b.alimentadorid,
                      GET_DESCRIPCION_ALIMENTADOR(b.alimentadorid) DescAlimentador,
                      f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo),
                      b.usuarioregistro             usr_registro,
                      b.fecharegistro               fec_registro,
                      b.usuariomodificacionregistro usr_mod_registro,
                      b.fechamodificacionregistro   fec_mod_registro
              FROM puestoseccionadorfusible b
              WHERE b.faseconexion is not null AND 
                    f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo) <> 'OK'
                
              UNION ALL
          
              SELECT 'Punto Carga' Elemento,
                      b.objectid,
                      b.alimentadorid,
                      GET_DESCRIPCION_ALIMENTADOR(b.alimentadorid) DescAlimentador,
                      f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo),
                      b.usuarioregistro usr_registro,
                      b.fecharegistro   fec_registro,
                      b.usuariomodificacionregistro usr_mod_registro,
                      b.fechamodificacionregistro fec_mod_registro
              FROM puntocarga b
              WHERE b.faseconexion is not null AND 
                    f_validar_FaseAlimentadorINFO(b.faseconexion, b.alimentadorinfo) <> 'OK' AND b.subtipo <> 1
         )F;

      Lc_Datos  C_Datos%ROWTYPE;

  -----
  BEGIN
  -----
    -- Error en la Configuración de la Fase y otros
    ---- Dominio FaseConexion  A=4, B=2, C=1, AC=5, AB=6, BC=3 y ABC = 7
    ----  Si la Fase es “A” el campo alimentador info debe ser “1”
    ----  Si la Fase es “B” el campo alimentador info debe ser “2”
    ----  Si la Fase es “C” el campo alimentador info debe ser “4”
    ----  Si la Fase es “AB” el campo alimentador info debe ser “3”
    ----  Si la Fase es “BC” el campo alimentador info debe ser “6”
    ----  Si la Fase es “AC” el campo alimentador info debe ser “5”
    ----  Si la Fase es “ABC” el campo alimentador info debe ser “7”

    Gv_DescERROR:= 'Errores del Feeder INFO y Lado Baja TR';

    FOR Lc_Datos IN C_Datos LOOP
      BEGIN
        INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
        VALUES('EFM', Gv_DescERROR, Lc_Datos.alimentadorid, Lc_Datos.DescAlimentador, Lc_Datos.Elemento, Lc_Datos.objectid, Lc_Datos.datoadicional1, Lc_Datos.datoadicional2, Lc_Datos.fechavalidacion, Lc_Datos.usr_registro, Lc_Datos.fec_registro, Lc_Datos.usr_mod_registro, Lc_Datos.fec_mod_registro);
      END;
    END LOOP;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_FeederInfo', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);

      COMMIT;
  END P_V_FeederInfo;
/******************************************************************************************/

                                        -- 9 --

/******************************************************************************************/
PROCEDURE P_V_BTconParentCSDisyuntor IS
/******************************************************************************************/

  CURSOR C_Datos IS
          SELECT  F.alimentadorid,
                  F.DescAlimentador,
                  F.Elemento,
                  F.objectid,
                  NULL datoadicional1,
                  NULL datoadicional2,
                  TRUNC(sysdate)  fechavalidacion,
                  F.usr_registro,
                  F.fec_registro,
                  F.usr_mod_registro,
                  F.fec_mod_registro
          FROM (
                SELECT 'Tramo Baja Aereo' Elemento,
                        b.objectid,
                        b.alimentadorid,
                        GET_DESCRIPCION_ALIMENTADOR(b.alimentadorid) DescAlimentador,
                        b.usuarioregistro usr_registro,
                        b.fecharegistro   fec_registro,
                        b.usuariomodificacionregistro usr_mod_registro,
                        b.fechamodificacionregistro fec_mod_registro
                FROM tramobajatensionaereo b
                INNER JOIN (
                            SELECT a.circuitsourceguid
                            FROM puestoprotecciondinamico a
                            WHERE a.circuitsourceguid IS NOT NULL AND 
                                  a.circuitsourceguid = a.parentcircuitsourceguid
                            ) c
                ON b.parentcircuitsourceguid = c.circuitsourceguid
              
                UNION ALL
              
                SELECT 'Tramo Baja Subterraneo' Elemento,
                        b.objectid,
                        b.alimentadorid,
                        GET_DESCRIPCION_ALIMENTADOR(b.alimentadorid) DescAlimentador,
                        b.usuarioregistro             usr_registro,
                        b.fecharegistro               fec_registro,
                        b.usuariomodificacionregistro usr_mod_registro,
                        b.fechamodificacionregistro   fec_mod_registro

                FROM tramobajatensionsubterraneo b
                INNER JOIN (
                            SELECT a.circuitsourceguid
                            FROM puestoprotecciondinamico a
                            WHERE a.circuitsourceguid IS NOT NULL AND 
                                  a.circuitsourceguid = a.parentcircuitsourceguid
                            ) c
                ON b.parentcircuitsourceguid = c.circuitsourceguid
                
                UNION ALL
                
                SELECT 'Punto Carga' Elemento,
                        b.objectid,
                        b.alimentadorid,
                        GET_DESCRIPCION_ALIMENTADOR(b.alimentadorid) DescAlimentador,
                        b.usuarioregistro             usr_registro,
                        b.fecharegistro               fec_registro,
                        b.usuariomodificacionregistro usr_mod_registro,
                        b.fechamodificacionregistro   fec_mod_registro

                FROM Puntocarga b
                INNER join (
                        SELECT a.circuitsourceguid
                        FROM puestoprotecciondinamico a
                        WHERE a.circuitsourceguid IS NOT NULL AND 
                              a.circuitsourceguid = a.parentcircuitsourceguid
                        ) c
                ON b.parentcircuitsourceguid = c.circuitsourceguid
                INNER JOIN conexionconsumidor a
                ON b.globalid = a.PUNTOCARGAGLOBALID
                WHERE b.subtipo <> 6  AND 
                      NOT(b.subtipo = 1 AND a.proyectoconstruccion = 'FOPE')  -- Que no sean Medidores con subtipo: 6 = Medio Voltaje y 1 = Totalizadores que tienen FOPE en proyectoconstruccion
                
                UNION ALL
                
                SELECT 'Luminaria' Elemento,
                        b.objectid,
                        b.alimentadorid,
                        GET_DESCRIPCION_ALIMENTADOR(b.alimentadorid) DescAlimentador,
                        b.usuarioregistro             usr_registro,
                        b.fecharegistro               fec_registro,
                        b.usuariomodificacionregistro usr_mod_registro,
                        b.fechamodificacionregistro   fec_mod_registro
                FROM Luminaria b
                INNER JOIN (
                            SELECT a.circuitsourceguid
                            FROM puestoprotecciondinamico a
                            WHERE a.circuitsourceguid IS NOT NULL AND 
                                  a.circuitsourceguid = a.parentcircuitsourceguid
                            ) c
                ON b.parentcircuitsourceguid = c.circuitsourceguid
          )F;

      Lc_Datos  C_Datos%ROWTYPE;


  BEGIN
    --Validación Elementos de BT con ParentCircuitSource del Disyuntor
    Gv_DescERROR:= 'Elementos de BT que tienen el ParentCircuitSource del Disyuntor y no el de un Transformador';

    FOR Lc_Datos IN C_Datos LOOP
      BEGIN
        INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
        VALUES('EFM', Gv_DescERROR, Lc_Datos.alimentadorid, Lc_Datos.DescAlimentador, Lc_Datos.Elemento, Lc_Datos.objectid, Lc_Datos.datoadicional1, Lc_Datos.datoadicional2, Lc_Datos.fechavalidacion, Lc_Datos.usr_registro, Lc_Datos.fec_registro, Lc_Datos.usr_mod_registro, Lc_Datos.fec_mod_registro);
      END;
    END LOOP;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_BTconParentCSDisyuntor', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);

      COMMIT;
  END P_V_BTconParentCSDisyuntor;



--Validaciones especificas para cada elemento
PROCEDURE P_V_SF_FaseEstrucVoltCorr IS
  BEGIN
    --Validación de la fase de conexion, voltaje, codigo de estructura y corriente

    Gv_DescERROR:= 'Fase de conexion, Voltaje, Codigo de Estructura y Corriente';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Seccionador Fusible',
           F.objectid,
           F.FaseyVoltaje || ';' || F.FaseyCodigoEstructura,
           F.SubtipoyCodigoEstructura || ';' || F.CorrienteyCodigoEstructura,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          select G.objectid,
                 G.alimentadorid,
                 G.DescAlimentador,
                 case when G.CantFase = '1' and G.Voltaje <> 7960 then 'Voltaje incorrecto es Monofásico'
                      when G.CantFase in ('2','3') and G.Voltaje <> 13800 then 'Voltaje incorrecto es Bifásico o Trifásico'
                      else null
                 end FaseyVoltaje,
                 case when G.CantFase = '1' and G.CantFaseCodEstructura <> '1' then 'Codigo estructura incorrecto es Monofásico'
                      when G.CantFase = '2' and G.CantFaseCodEstructura <> '2' then 'Codigo estructura incorrecto es Bifásico'
                      when G.CantFase = '3' and G.CantFaseCodEstructura <> '3' then 'Codigo estructura incorrecto es Trifásico'
                      else null
                 end FaseyCodigoEstructura,
                 case when G.subtipo = 1 and G.SubtipoCodEstructura <> 'S' then 'Codigo Estructura incorrecto Unipolar Abierto'
                      when G.subtipo = 2 and G.SubtipoCodEstructura <> 'D' then 'Codigo estructura incorrecto Unipolar Cerrado'
                      when G.subtipo = 3 and G.SubtipoCodEstructura <> 'E' then 'Codigo estructura incorrecto Unipolar Abierto con Rompe arco'
                      else null
                 end SubtipoyCodigoEstructura,
                 case when G.subtipo = 1 and G.CorrienteCodEstructura <> G.Corriente then 'Codigo Estructura incorrecto (Corriente), Secc Fusible Unipolar Abierto'
                      when G.subtipo = 2 and G.CorrienteCodEstructura <> G.Corriente then 'Codigo estructura incorrecto (Corriente), Secc Fusible Unipolar Cerrado'
                      when G.subtipo = 3 and G.CorrienteCodEstructura <> G.Corriente then 'Codigo estructura incorrecto (Corriente), Secc Fusible Unipolar Abierto con Rompe arco'
                      else null
                 end CorrienteyCodigoEstructura,
                 G.usr_registro,
                 G.fec_registro,
                 G.usr_mod_registro,
                 G.fec_mod_registro
          from (
                select ps.objectid,
                       ps.subtipo,
                       case
                         when ps.subtipo = 1 then 'Unipolar Abierto'
                         when ps.subtipo = 2 then 'Unipolar Cerrado'
                         when ps.subtipo = 3 then 'Unipolar Abierto con Dispositivo Rompe Arco'
                       end DescSubtipo,
                       ps.alimentadorid,
                       get_descripcion_alimentador(ps.alimentadorid) DescAlimentador,
                       ps.alimentadorinfo,
                       ps.voltaje,
                       ps.faseconexion,
                       case
                         when FaseConexion in (1,2,4) then '1' --Monofásico
                         when FaseConexion in (3,5,6) then '2' --Bifásico
                         when FaseConexion = 7 then '3' --Trifásico
                       end CantFase,
                       ps.codigoestructura,
                       c.descripcioncorta,
                       substr(c.descripcioncorta,1,1) CantFaseCodEstructura,
                       substr(c.descripcioncorta,2,1) SubtipoCodEstructura, --S = Unipolar abierto, E = Unipolar abierto con rompe arco y  D = Unipolar Cerrado
                       substr(c.descripcioncorta,3,3) CorrienteCodEstructura,
                       ps.corriente,
                       ps.usuarioregistro             usr_registro,
                       ps.fecharegistro               fec_registro,
                       ps.usuariomodificacionregistro usr_mod_registro,
                       ps.fechamodificacionregistro   fec_mod_registro
                from puestoseccionadorfusible ps
                 left outer join  catalogoestructura c
                   on ps.codigoestructura = c.codigoestructura
               )G
         )F
    WHERE FaseyCodigoEstructura IS NOT NULL OR 
          F.FaseyVoltaje IS NOT NULL OR 
          F.SubtipoyCodigoEstructura IS NOT NULL OR
          F.CorrienteyCodigoEstructura IS NOT NULL;

    COMMIT;

  EXCEPTION
     WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_SF_FaseEstrucVoltCorr', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_SF_FaseEstrucVoltCorr;


  PROCEDURE P_V_SF_FaseConexionyUnidadFusi IS
  BEGIN
    --Validación de la cantidad de unidades creadas en la tabla relacional, de acuerdo a la Fase de Conexion que tiene asiganada en el Puesto

    Gv_DescERROR:= 'Cantidad de unidades (tabla relacional) de acuerdo a la Fase Conexión';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Seccionador Fusible',
           F.objectid,
           'Cantidad por Fase Conexion: ' || F.CantidadFase,
           'Cantidad en Unidad Fusible: ' || F.CantidadUnidad,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          select pfu.alimentadorid,
                 get_descripcion_alimentador(pfu.alimentadorid) DescAlimentador,
                 pfu.objectid,
                 pfu.globalid,
                 pfu.CantidadFase,
                 pfu.CantidadUnidad,
                 pfu.usr_registro,
                 pfu.fec_registro,
                 pfu.usr_mod_registro,
                 pfu.fec_mod_registro
          from (
                select psf.alimentadorid,
                       psf.objectid,
                       psf.globalid,
                       case
                         when psf.FaseConexion in (1,2,4) then '1' --Monofásico
                         when psf.FaseConexion in (3,5,6) then '2' --Bifásico
                         when psf.FaseConexion = 7 then '3' --Trifásico
                       end CantidadFase,
                       count(usf.objectid)                CantidadUnidad,
                       psf.usuarioregistro                usr_registro,
                       psf.fecharegistro                  fec_registro,
                       psf.usuariomodificacionregistro    usr_mod_registro,
                       psf.fechamodificacionregistro      fec_mod_registro
                from Puestoseccionadorfusible psf
                 left outer join unidadfusible usf
                    on psf.globalid = usf.puestosecfusibleglobalid
                group by psf.alimentadorid, psf.objectid, psf.globalid, psf.FaseConexion, psf.usuarioregistro, psf.fecharegistro, psf.usuariomodificacionregistro, psf.fechamodificacionregistro
                )pfu
          where pfu.CantidadFase <> pfu.CantidadUnidad
         )F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_SF_FaseConexionyUnidadFusi', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_SF_FaseConexionyUnidadFusi;


 PROCEDURE P_V_SF_CapacidadConValorNULL IS
  BEGIN
    --Validación de la Capacidad de la unidad fusible con el valor en NULL

    Gv_DescERROR:= 'Capacidad en Unidad Fusible con valor NULL';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Seccionador Fusible',
           F.objectid,
           F.CapacidadUF,
           NULL,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          Select psf.alimentadorid,
                 GET_DESCRIPCION_ALIMENTADOR(psf.alimentadorid) DescAlimentador,
                 psf.objectid,
                 case when nvl(usf.capacidad,0) = 0 then 'Ingresar Capacidad en unidad fusible' else '' end CapacidadUF,
                 psf.usuarioregistro                    usr_registro, 
                 psf.fecharegistro                      fec_registro,
                 psf.usuariomodificacionregistro        usr_mod_registro,
                 psf.fechamodificacionregistro          fec_mod_registro
          from Puestoseccionadorfusible psf
           inner join unidadfusible usf
             on psf.globalid = usf.puestosecfusibleglobalid
          where usf.CAPACIDAD IS NULL
         )F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_SF_CapacidadConValorNULL', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_SF_CapacidadConValorNULL;


  PROCEDURE P_V_CA_FaseConexionyUnidad IS
  BEGIN
    --Validación de la cantidad de unidades creadas en la tabla relacional, de acuerdo a la Fase de Conexion que tiene asiganada en el Puesto

    Gv_DescERROR:= 'Cantidad de Unidades (tabla relacional) de acuerdo a la Fase Conexión';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Capacitor',
           F.objectid,
           'Cantidad por Fase Conexion: ' || F.CantidadFase,
           'Cantidad en Unidad Fusible: ' || F.CantidadUnidad,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro

    FROM (
          select pcu.alimentadorid,
                 get_descripcion_alimentador(pcu.alimentadorid) DescAlimentador,
                 pcu.objectid,
                 pcu.globalid,
                 pcu.CantidadFase,
                 pcu.CantidadUnidad,
                 pcu.usr_registro, 
                 pcu.fec_registro,
                 pcu.usr_mod_registro,
                 pcu.fec_mod_registro
          from (
                Select pc.alimentadorid,
                       pc.objectid,
                       pc.globalid,
                       case
                         when pc.FaseConexion in (1,2,4) then '1' --Monofásico
                         when pc.FaseConexion in (3,5,6) then '2' --Bifásico
                         when pc.FaseConexion = 7 then '3' --Trifásico
                       end CantidadFase,
                       count(uc.objectid) CantidadUnidad,
                       pc.usuarioregistro                    usr_registro, 
                       pc.fecharegistro                      fec_registro,
                       pc.usuariomodificacionregistro        usr_mod_registro,
                       pc.fechamodificacionregistro          fec_mod_registro
                from Puestocorrectorfactorpotencia pc
                 left outer join Unidadcapacitor uc
                    on pc.globalid = uc.puestocorrfacpotglobalid
                group by pc.alimentadorid, pc.objectid, pc.globalid, pc.FaseConexion, pc.usuarioregistro, pc.fecharegistro, pc.usuariomodificacionregistro, pc.fechamodificacionregistro
               )pcu
          where pcu.CantidadFase <> pcu.CantidadUnidad
         )F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_CA_FaseConexionyUnidad', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_CA_FaseConexionyUnidad;


  PROCEDURE P_V_CA_PotenciaUnidadyPuesto IS
  BEGIN
    --Validación, Suma de la Potencia Nominal de las unidades deben ser igual a la Potencia kva del Puesto

    Gv_DescERROR:= 'Suma de Potencia en tabla relacional igual a la Potencia del Puesto';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Capacitor',
           F.objectid,
           F.PotenciakvaPuestoUnidad || '. Puesto = ' || F.potenciakva || ' Unidad = ' || F.potencianominalunidad,
           NULL,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          select G.alimentadorid,
                 GET_DESCRIPCION_ALIMENTADOR(G.alimentadorid) DescAlimentador,
                 G.objectid,
                 G.potenciakva,
                 G.potencianominalunidad,
                 case when G.potenciakva <> G.potencianominalunidad then 'Potencia kva del Puesto es diferente al de la Unidad' else null end PotenciakvaPuestoUnidad,
                 G.usr_registro,
                 G.fec_registro,
                 G.usr_mod_registro,
                 G.fec_mod_registro
          from
               (
                select pc.alimentadorid,
                       pc.objectid,
                       round(pc.potenciakva,2) potenciakva,
                       round(sum(replace(nvl(uc.potencianominal,0), '.',',')),2) potencianominalunidad,
                       pc.usuarioregistro                    usr_registro, 
                       pc.fecharegistro                      fec_registro,
                       pc.usuariomodificacionregistro        usr_mod_registro,
                       pc.fechamodificacionregistro          fec_mod_registro
                from Puestocorrectorfactorpotencia pc
                 left outer join Unidadcapacitor uc
                    on pc.globalid = uc.puestocorrfacpotglobalid
                group by pc.alimentadorid, pc.objectid, round(pc.potenciakva,2), pc.usuarioregistro, pc.fecharegistro, pc.usuariomodificacionregistro, pc.fechamodificacionregistro
               )G
           where G.potenciakva <> G.potencianominalunidad
         )F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_CA_PotenciaUnidadyPuesto', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_CA_PotenciaUnidadyPuesto;


  PROCEDURE P_V_TM_ConductorConfySecFase IS
  BEGIN
    --Validación Codigo de estructura (fase y neutro) y Configuracion de conductores con valor NULL y Secuencia Fase con el respectivo valor

    Gv_DescERROR:= 'Codigo de Estructura, Configuracion de conductores y Secuencia de Fase';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           F.Elemento,
           F.objectid,
           F.Observacion,
           NULL,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          select 'TMA' Elemento,
                 tda.alimentadorid,
                 get_descripcion_alimentador(tda.alimentadorid) DescAlimentador,
                 tda.objectid,
                 'Ingresar valor en Código Conductor Fase, configuración y secuencia de fase' Observacion,
                 tda.usuarioregistro                    usr_registro, 
                 tda.fecharegistro                      fec_registro,
                 tda.usuariomodificacionregistro        usr_mod_registro,
                 tda.fechamodificacionregistro          fec_mod_registro
          from tramodistribucionaereo tda
          where tda.codigoconductorfase is NULL or
                tda.configuracionconductores is NULL or
                tda.secuenciafase is NULL
          union all
          select 'TMS' Elemento,
                 tds.alimentadorid,
                 get_descripcion_alimentador(tds.alimentadorid) DescAlimentador,
                 tds.objectid,
                 'Ingresar valor en Código Conductor Fase, configuración y secuencia de fase' Observacion,
                 tds.usuarioregistro                    usr_registro, 
                 tds.fecharegistro                      fec_registro,
                 tds.usuariomodificacionregistro        usr_mod_registro,
                 tds.fechamodificacionregistro          fec_mod_registro
          from tramodistribucionsubterraneo tds
          where tds.codigoconductorfase is NULL or
                tds.configuracionconductores is NULL or
                tds.secuenciafase is NULL
          union all
          --Secuencia de fase debe coincidir con la fase asignada al tramo
          select tm.elemento,
                 tm.alimentadorid,
                 get_descripcion_alimentador(tm.alimentadorid) DescAlimentador,
                 tm.objectid,
                 'Atributo SECUENCIAFASE tiene un valor que no corresponde con la FASE definida en el Tramo' Observacion,
                 tm.usr_registro, 
                 tm.fec_registro,
                 tm.usr_mod_registro,
                 tm.fec_mod_registro
          from (
                select 'TMA' Elemento,
                       tda.alimentadorid,
                       tda.objectid,
                       case when tda.faseconexion = 4 then 'A'
                            when tda.faseconexion = 2 then 'B'
                            when tda.faseconexion = 1 then 'C'
                            when tda.faseconexion = 5 then 'AC'
                            when tda.faseconexion = 6 then 'AB'
                            when tda.faseconexion = 3  then 'BC'
                            when tda.faseconexion = 7  then 'ABC'
                       end FaseConexionTM,
                       tda.secuenciafase,
                       tda.usuarioregistro                    usr_registro, 
                       tda.fecharegistro                      fec_registro,
                       tda.usuariomodificacionregistro        usr_mod_registro,
                       tda.fechamodificacionregistro          fec_mod_registro
                from tramodistribucionaereo tda
                where tda.faseconexion is NOT NULL AND
                      tda.secuenciafase is NOT NULL
                union all
                select 'TMS' Elemento,
                       tds.alimentadorid,
                       tds.objectid,
                       case when tds.faseconexion = 4 then 'A'
                            when tds.faseconexion = 2 then 'B'
                            when tds.faseconexion = 1 then 'C'
                            when tds.faseconexion = 5 then 'AC'
                            when tds.faseconexion = 6 then 'AB'
                            when tds.faseconexion = 3  then 'BC'
                            when tds.faseconexion = 7  then 'ABC'
                       end FaseConexionTM,
                       tds.secuenciafase,
                       tds.usuarioregistro                 usr_registro, 
                       tds.fecharegistro                   fec_registro,
                       tds.usuariomodificacionregistro     usr_mod_registro,
                       tds.fechamodificacionregistro       fec_mod_registro
                from tramodistribucionsubterraneo tds
                where tds.faseconexion is NOT NULL AND
                      tds.secuenciafase is NOT NULL
               )tm
          where Length(TRIM(translate(tm.secuenciafase,tm.FaseConexionTM, '   '))) > 0
         )F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_TM_ConductorConfySecFase', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_TM_ConductorConfySecFase;


 PROCEDURE P_V_TM_ConductorFaseyVoltaje IS
  BEGIN
    --Validación Código de estructura (fase y neutro), fase de conexion, voltaje y configuración de conductores

    Gv_DescERROR:= 'Codigo Estructura, Fase de Conexion, Voltaje y Configuración de Conductores';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           F.Elemento,
           F.objectid,
           F.FaseyVoltaje,
           F.FaseyConfiguracionConductor || ';' || F.NeutroyConfiguracionConductor,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          select G.Elemento,
                 G.objectid,
                 G.alimentadorid,
                 G.DescAlimentador,
                 case when G.CantFase = 1 and G.Voltaje <> 7960 then 'Voltaje incorrecto es Monofásico'
                      when G.CantFase in (2,3) and G.Voltaje <> 13800 then 'Voltaje incorrecto es Bifásico o Trifásico'
                      else null
                 end FaseyVoltaje,
                 case when G.CantFase = 1 and substr(G.configuracionconductores,1,1) <> '1' then 'Configuracion de Conductor incorrecto es Monofásico'
                      when G.CantFase = 2 and substr(G.configuracionconductores,1,1) <> '2' then 'Configuracion de Conductor incorrecto es Bifásico'
                      when G.CantFase = 3 and substr(G.configuracionconductores,1,1) <> '3' then 'Configuracion de Conductor incorrecto es Trifásico'
                      else null
                 end FaseyConfiguracionConductor,
                 case when G.CantNeutro = 0 and to_number(substr(G.configuracionconductores,2,1)) <> G.CantFase then 'Configuracion de Conductor incorrecto no tiene Neutro'
                      when G.CantNeutro = 1 and to_number(substr(G.configuracionconductores,2,1)) < (G.CantFase + G.CantNeutro) then 'Configuracion de Conductor incorrecto tiene Neutro'
                      else null
                 end NeutroyConfiguracionConductor,
                 G.usr_registro, 
                 G.fec_registro,
                 G.usr_mod_registro,
                 G.fec_mod_registro
          from (
                select tda.Elemento,
                       tda.objectid,
                       tda.alimentadorid,
                       get_descripcion_alimentador(tda.alimentadorid) DescAlimentador,
                       case
                         when tda.FaseConexion in (1,2,4) then 1 --Monofásico
                         when tda.FaseConexion in (3,5,6) then 2 --Bifásico
                         when tda.FaseConexion = 7 then 3 --Trifásico
                           else 0
                       end CantFase,
                       tda.voltaje,
                       tda.codigoconductorfase,
                       c.descripcioncorta DescCortaFase,
                       tda.codigoconductorneutro,
                       DECODE(nvl(tda.codigoconductorneutro, 'COO0000'), 'COO0000', 0, 1) CantNeutro,
                       c1.descripcioncorta DescCortaNeutro,
                       nvl(tda.configuracionconductores, '0') configuracionconductores,
                       case
                         when tda.configuracionconductores = '11' then '1F1C'
                         when tda.configuracionconductores = '12' then '1F2C'
                         when tda.configuracionconductores = '13' then '1F3C'
                         when tda.configuracionconductores = '14' then '1F4C'
                         when tda.configuracionconductores = '22' then '2F2C'
                         when tda.configuracionconductores = '23' then '2F3C'
                         when tda.configuracionconductores = '24' then '2F4C'
                         when tda.configuracionconductores = '25' then '2F5C'
                         when tda.configuracionconductores = '33' then '3F3C'
                         when tda.configuracionconductores = '34' then '3F4C'
                         when tda.configuracionconductores = '35' then '3F5C'
                           else ''
                       end DescConfiguracionConductores, 
                       tda.usuarioregistro                 usr_registro, 
                       tda.fecharegistro                   fec_registro, 
                       tda.usuariomodificacionregistro     usr_mod_registro,
                       tda.fechamodificacionregistro       fec_mod_registro
                from (
                       Select 'MTA' Elemento,
                              ta.objectid,
                              ta.subtipo,
                              case
                                   when ta.subtipo = 1 then 'Tramo MTA Monofásico'
                                   when ta.subtipo = 2 then 'Tramo MTA Bifásico'
                                   when ta.subtipo = 3 then 'Tramo MTA Trifásico'
                                   when ta.subtipo = 4 then 'Bajante MTA Monofásico'
                                   when ta.subtipo = 5 then 'Bajante MTA Bifásico'
                                   when ta.subtipo = 6 then 'Bajante MTA Trifásico'
                              end DescSubtipo,
                              ta.alimentadorid,
                              ta.alimentadorinfo,
                              ta.faseconexion,
                              ta.codigoconductorneutro,
                              ta.voltaje,
                              ta.codigoconductorfase,
                              ta.configuracionconductores,
                              ta.usuarioregistro, 
                              ta.fecharegistro, 
                              ta.usuariomodificacionregistro,
                              ta.fechamodificacionregistro
                       from tramodistribucionaereo ta
                       union all
                       Select 'MTS',
                              ts.objectid,
                              ts.subtipo,
                              case
                                   when ts.subtipo = 1 then 'Tramo MTS Monofásico'
                                   when ts.subtipo = 2 then 'Tramo MTS Bifásico'
                                   when ts.subtipo = 3 then 'Tramo MTS Trifásico'
                                   when ts.subtipo = 4 then 'Bajante MTS Monofásico'
                                   when ts.subtipo = 5 then 'Bajante MTS Bifásico'
                                   when ts.subtipo = 6 then 'Bajante MTS Trifásico'
                              end DescSubtipo,
                              ts.alimentadorid,
                              ts.alimentadorinfo,
                              ts.faseconexion,
                              ts.codigoconductorneutro,
                              ts.voltaje,
                              ts.codigoconductorfase,
                              ts.configuracionconductores,
                              ts.usuarioregistro, 
                              ts.fecharegistro, 
                              ts.usuariomodificacionregistro,
                              ts.fechamodificacionregistro
                       from tramodistribucionsubterraneo ts
                     ) tda
                 left outer join catalogoestructura c
                   on tda.codigoconductorfase = c.codigoestructura
                 left outer join catalogoestructura c1
                   on tda.codigoconductorneutro = c1.codigoestructura
                where tda.codigoconductorfase is not null
                  and tda.codigoconductorneutro is not null
                  and tda.FaseConexion is not null
                  and tda.configuracionconductores is not null
               )G
         )F
     where F.FaseyVoltaje is not null or
           F.FaseyConfiguracionConductor is not null or
           F.NeutroyConfiguracionConductor is not null;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_TM_ConductorFaseyVoltaje', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_TM_ConductorFaseyVoltaje;


  PROCEDURE P_V_PD_CorrienteFaseyVoltaje IS
  BEGIN
    --Validación de Corriente, Corriente Maxima, Fase conexion y Voltaje

    Gv_DescERROR:= 'Corriente, Corriente Máxima, Fase Conexion y Voltaje con valor NULL';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Puesto Proteccion Dinámico',
           F.objectid,
           F.Corriente || ';' || F.CorrienteMaxCC,
           F.FaseConexion || ';' || F.Voltaje,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          select ppd.alimentadorid,
                 get_descripcion_alimentador(ppd.alimentadorid) DescAlimentador,
                 ppd.objectid,
                 case when ppd.corriente is NULL then 'Valor de Corriente inválido' else '' end Corriente,
                 case when ppd.corrientemaxcortocircuito is NULL then 'Valor de Corriente Max Corto Circuito inválido' else '' end CorrienteMaxCC,
                 case when ppd.faseconexion is NULL then 'No tiene Fase especificada' else '' end FaseConexion,
                 case when ppd.voltaje is NULL then 'Valor de Voltaje inválido' else '' end Voltaje,
                 ppd.usuarioregistro              usr_registro, 
                 ppd.fecharegistro                fec_registro, 
                 ppd.usuariomodificacionregistro  usr_mod_registro,
                 ppd.fechamodificacionregistro    fec_mod_registro
          from Puestoprotecciondinamico ppd
          where ppd.corriente is NULL OR
                ppd.corrientemaxcortocircuito is NULL OR
                ppd.faseconexion is NULL OR
                ppd.voltaje is NULL
         )F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_PD_CorrienteFaseyVoltaje', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_PD_CorrienteFaseyVoltaje;

  PROCEDURE P_V_PT_NTapsyConfLadoBajaMedia IS
  BEGIN
    --Validación de Numero de Taps válido (1-5) y Configuración Lado Baja o Media

    Gv_DescERROR:= 'Número de Taps y Configuración Lado Baja-Media';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Puesto Transformador',
           F.objectid,
           F.Observacion,
           NULL,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          select pt.AlimentadorID,
                 get_descripcion_alimentador(pt.AlimentadorID) DescAlimentador,
                 pt.objectid,
                 'Valor invalido de Tap Normal, permitido: 1-5' Observacion,
                 pt.usuarioregistro               usr_registro, 
                 pt.fecharegistro                 fec_registro, 
                 pt.usuariomodificacionregistro   usr_mod_registro,
                 pt.fechamodificacionregistro     fec_mod_registro
          from Puestotransfdistribucion pt
           inner join unidadtransfdistribucion ut
              on pt.globalid = ut.puestotransfdistglobalid
          where ut.tapnormal is null or (ut.tapnormal < 1 or ut.tapnormal > 5)
          UNION ALL
          select pt.alimentadorid,
                 get_descripcion_alimentador(pt.AlimentadorID),
                 pt.objectid,
                 'Configuración lado baja ó media, vacío',
                 pt.usuarioregistro               usr_registro, 
                 pt.fecharegistro                 fec_registro, 
                 pt.usuariomodificacionregistro   usr_mod_registro,
                 pt.fechamodificacionregistro     fec_mod_registro
          from Puestotransfdistribucion pt
           inner join unidadtransfdistribucion ut
             on pt.globalID = ut.puestotransfdistglobalid
          where pt.configuracionladobaja is null or pt.configuracionladomedia is null
         )F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_PT_NTapsyConfLadoBajaMedia', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_PT_NTapsyConfLadoBajaMedia;

  PROCEDURE P_V_PT_UnidadySubtipo IS
  BEGIN
    --Validación de la cantidad de unidades de acuerdo al subtipo del Transformador

    Gv_DescERROR:= 'Cantidad de Unidades (tabla relacional) de acuerdo al Subtipo del Transformador';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Puesto Transformador',
           F.objectid,
           F.Subtipo || ' creado con ' || F.cantidadunidad || ' unidad(es)',
           'Debe tener ' || cantidadrequerida || ' unidad(es)',
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          select case when ptu.subtipo in (1,2,3,4) then 'Monofasico'
                      when ptu.subtipo in (13) then 'Bifasico'
                      when ptu.subtipo in (5,6,7,8) then 'Trifasico'
                 end Subtipo,
                 ptu.alimentadorid,
                 get_descripcion_alimentador(ptu.AlimentadorID) DescAlimentador,
                 ptu.objectid,
                 ptu.cantidadunidad,
                 1 cantidadrequerida,
                 ptu.usr_registro, 
                 ptu.fec_registro, 
                 ptu.usr_mod_registro,
                 ptu.fec_mod_registro
          from (
                select pt.alimentadorid, 
                       pt.subtipo, 
                       pt.objectid, 
                       count(ut.objectid)             cantidadunidad,
                       pt.usuarioregistro             usr_registro, 
                       pt.fecharegistro               fec_registro, 
                       pt.usuariomodificacionregistro usr_mod_registro,
                       pt.fechamodificacionregistro   fec_mod_registro
                from Puestotransfdistribucion pt
                inner join unidadtransfdistribucion ut
                    on pt.globalid = ut.puestotransfdistglobalid
                where pt.subtipo in (1,2,3,4,5,6,7,8,13)
                group by pt.alimentadorid, pt.subtipo, pt.objectid, pt.usuarioregistro, pt.fecharegistro, pt.usuariomodificacionregistro, pt.fechamodificacionregistro
               ) ptu
          where ptu.cantidadunidad  <> 1
          UNION ALL
          select 'Banco 2 Transformadores',
                 ptu.alimentadorid,
                 get_descripcion_alimentador(ptu.AlimentadorID) DescAlimentador,
                 ptu.objectid,
                 ptu.cantidadunidad,
                 2,
                 ptu.usr_registro, 
                 ptu.fec_registro, 
                 ptu.usr_mod_registro,
                 ptu.fec_mod_registro
          from (
                select pt.alimentadorid, pt.objectid, count(ut.objectid) cantidadunidad,
                       pt.usuarioregistro             usr_registro, 
                       pt.fecharegistro               fec_registro, 
                       pt.usuariomodificacionregistro usr_mod_registro,
                       pt.fechamodificacionregistro   fec_mod_registro
                from Puestotransfdistribucion pt
                 inner join unidadtransfdistribucion ut
                    on pt.globalid = ut.puestotransfdistglobalid
                where pt.subtipo in (9,10)
                group by pt.alimentadorid, pt.objectid, pt.usuarioregistro, pt.fecharegistro, pt.usuariomodificacionregistro, pt.fechamodificacionregistro
                )ptu
          where ptu.cantidadunidad  <> 2
          UNION ALL
          select 'Banco 3 Transformadores',
                 ptu.alimentadorid,
                 get_descripcion_alimentador(ptu.AlimentadorID) DescAlimentador,
                 ptu.objectid,
                 ptu.cantidadunidad,
                 3,
                 ptu.usr_registro, 
                 ptu.fec_registro, 
                 ptu.usr_mod_registro,
                 ptu.fec_mod_registro
          from (
                select pt.alimentadorid, pt.objectid, count(ut.objectid) cantidadunidad,
                       pt.usuarioregistro usr_registro, 
                       pt.fecharegistro fec_registro, 
                       pt.usuariomodificacionregistro usr_mod_registro,
                       pt.fechamodificacionregistro fec_mod_registro
                from Puestotransfdistribucion pt
                 inner join unidadtransfdistribucion ut
                    on pt.globalid = ut.puestotransfdistglobalid
                where pt.subtipo in (11,12)
                group by pt.alimentadorid, pt.objectid, pt.usuarioregistro, pt.fecharegistro, pt.usuariomodificacionregistro, pt.fechamodificacionregistro
                )ptu
          where ptu.cantidadunidad  <> 3
         )F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_PT_UnidadySubtipo', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_PT_UnidadySubtipo;


  PROCEDURE P_V_PT_FaseConexPuestoyUnidad IS

  BEGIN
    --Validación Fase de Conexion del Puesto con la Unidad

    Gv_DescERROR:= 'Fase de Conexión del Puesto y la Unidad deben coincidir';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Puesto Transformador',
           F.objectid,
           F.Subtipo || ' no coinciden Fase de Conexión, Puesto=' || F.FaseConexionPTDesc  || '  Unidad=' || F.FaseConexionUTDesc,
           F.Nota,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          select t.AlimentadorID,
                 get_descripcion_alimentador(t.AlimentadorID) DescAlimentador,
                 case when ptu.subtipo in (1,2,3,4) then 'Monofasico'
                      when ptu.subtipo in (13) then 'Bifasico'
                      when ptu.subtipo in (5,6,7,8) then 'Trifasico'
                      when ptu.subtipo in (9,10) then 'Banco 2 Transformadores'
                      when ptu.subtipo in (11,12) then 'Banco 3 Transformadores'
                 end Subtipo,
                 ptu.objectid,
                 case when ptu.FaseConexionPT = 0 then 'Fase NULL'
                      when ptu.FaseConexionPT = 4 then 'A'
                      when ptu.FaseConexionPT = 2 then 'B'
                      when ptu.FaseConexionPT = 1 then 'C'
                      when ptu.FaseConexionPT = 5 then 'AC'
                      when ptu.FaseConexionPT = 6 then 'AB'
                      when ptu.FaseConexionPT = 3  then 'BC'
                      when ptu.FaseConexionPT = 7  then 'ABC'
                 end FaseConexionPTDesc,
                 case when ptu.FaseConexionUT = 0 then 'Fase NULL'
                      when ptu.FaseConexionUT = 4 then 'A'
                      when ptu.FaseConexionUT = 2 then 'B'
                      when ptu.FaseConexionUT = 1 then 'C'
                      when ptu.FaseConexionUT = 5 then 'AC'
                      when ptu.FaseConexionUT = 6 then 'AB'
                      when ptu.FaseConexionUT = 3  then 'BC'
                      when ptu.FaseConexionUT = 7  then 'ABC'
                     else '--'
                 end FaseConexionUTDesc,
                 case when (ptu.CantUnidad <> 0 and ptu.CantFasePuesto = 1) then case when ptu.CantUnidad > ptu.CantFasePuesto then 'Ignorar si es Transformador en Paralelo' end end Nota,
                 ptu.usr_registro, 
                 ptu.fec_registro, 
                 ptu.usr_mod_registro,
                 ptu.fec_mod_registro
          from (
                select pt.subtipo,
                       pt.objectid,
                       nvl(pt.FaseConexion,0)      FaseConexionPT,
                       case when pt.FaseConexion in (1,2,4) then 1 --Monofásico
                            when pt.FaseConexion in (3,5,6) then 2 --Bifásico
                            when pt.FaseConexion = 7 then 3 --Trifásico
                            else 0
                       end CantFasePuesto,
                       sum(nvl(ut.FaseConexion,0)) FaseConexionUT,
                       count(ut.objectid) CantUnidad,
                       pt.usuarioregistro usr_registro, 
                       pt.fecharegistro fec_registro, 
                       pt.usuariomodificacionregistro usr_mod_registro,
                       pt.fechamodificacionregistro fec_mod_registro
                from Puestotransfdistribucion pt
                 left outer join unidadtransfdistribucion ut
                    on pt.globalid = ut.puestotransfdistglobalid
                group by pt.subtipo, pt.objectid, pt.FaseConexion, pt.usuarioregistro, pt.fecharegistro, pt.usuariomodificacionregistro, pt.fechamodificacionregistro
                order by pt.objectid
                )ptu
          inner join Puestotransfdistribucion t
            on ptu.objectid = t.objectid
          where ptu.FaseConexionPT  <> ptu.FaseConexionUT or  ptu.FaseConexionPT = 0
         )F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_PT_FaseConexPuestoyUnidad', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_PT_FaseConexPuestoyUnidad;

  PROCEDURE P_V_PT_FaseConexionBancoTR IS
  BEGIN
    --Validación para verificación de la Fase de conexión del Puesto con la Fase de la Unidad,
    --para el caso de los Bancos Transformadores que tienen más de 1 unidad

    Gv_DescERROR:= 'Fase Conexión del Puesto y la Unidad para Banco Transformador';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Puesto Transformador',
           F.objectid,
           F.Subtipo || ' tiene Fase Conexion = ' || F.FaseConexionPT,
           'Fase ' || F.FaseConexionUT || ' se repite en la unidad ' || CantidadFaseRepetidaUT || ' veces;' || 'Ignorar si es Transformador en Paralelo',
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          select t.AlimentadorID,
                 get_descripcion_alimentador(t.AlimentadorID) DescAlimentador,
                 case when ptu.subtipo in (9,10) then 'Banco 2 Transformadores'
                      when ptu.subtipo in (11,12) then 'Banco 3 Transformadores'
                 end Subtipo,
                 ptu.objectid,
                 ptu.FaseConexionPT,
                 ptu.FaseConexionUT,
                 ptu.CantidadFaseRepetidaUT,
                 ptu.usr_registro,
                 ptu.fec_registro,
                 ptu.usr_mod_registro,
                 ptu.fec_mod_registro
          from (
                select pt.subtipo,
                       pt.objectid,
                       case when pt.FaseConexion = 4 then 'A'
                            when pt.FaseConexion = 2 then 'B'
                            when pt.FaseConexion = 1 then 'C'
                            when pt.FaseConexion = 5 then 'AC'
                            when pt.FaseConexion = 6 then 'AB'
                            when pt.FaseConexion = 3  then 'BC'
                            when pt.FaseConexion = 7  then 'ABC'
                       end FaseConexionPT,
                       case when ut.FaseConexion = 4 then 'A'
                            when ut.FaseConexion = 2 then 'B'
                            when ut.FaseConexion = 1 then 'C'
                            when ut.FaseConexion = 5 then 'AC'
                            when ut.FaseConexion = 6 then 'AB'
                            when ut.FaseConexion = 3  then 'BC'
                            when ut.FaseConexion = 7  then 'ABC'
                       end FaseConexionUT,
                       count(*) CantidadFaseRepetidaUT,
                       pt.usuarioregistro usr_registro, 
                       pt.fecharegistro fec_registro, 
                       pt.usuariomodificacionregistro usr_mod_registro,
                       pt.fechamodificacionregistro fec_mod_registro
                from Puestotransfdistribucion pt
                 inner join unidadtransfdistribucion ut
                    on pt.globalid = ut.puestotransfdistglobalid
                where pt.subtipo in (9,10,11,12)
                group by pt.subtipo, pt.objectid, pt.FaseConexion, ut.faseconexion, pt.usuarioregistro, pt.fecharegistro, pt.usuariomodificacionregistro, pt.fechamodificacionregistro
                )ptu
          inner join Puestotransfdistribucion t
            on ptu.objectid = t.objectid
          where ptu.CantidadFaseRepetidaUT  > 1
         )F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_PT_FaseConexionBancoTR', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_PT_FaseConexionBancoTR;

  PROCEDURE P_V_PT_PotenciaUnidadyPuesto IS
  BEGIN
    --Validación, Suma de la Potencia Nominal de las unidades deben ser igual a la Potencia kva del Puesto

    Gv_DescERROR:= 'Suma de Potencia en tabla relacional igual a la Potencia del Puesto';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Puesto Transformador',
           F.objectid,
           F.PotenciakvaPuestoUnidad || '. Puesto = ' || F.potenciakva || ' Unidad = ' || F.potencianominalunidad,
           NULL,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro

    FROM (
          select G.alimentadorid,
                 GET_DESCRIPCION_ALIMENTADOR(G.alimentadorid) DescAlimentador,
                 G.objectid,
                 G.potenciakva,
                 G.potencianominalunidad,
                 case when G.potenciakva <> G.potencianominalunidad then 'Potencia kva del Puesto es diferente al de la Unidad' else null end PotenciakvaPuestoUnidad,
                 G.usr_registro, 
                 G.fec_registro, 
                 G.usr_mod_registro,
                 G.fec_mod_registro
          from
               (
                select pt.alimentadorid,
                       pt.objectid,
                       round(pt.potenciakva,2) potenciakva,
                       round(sum(nvl(ut.potencianominal,0)),2) potencianominalunidad, -- OJO
                       pt.usuarioregistro usr_registro, 
                       pt.fecharegistro fec_registro, 
                       pt.usuariomodificacionregistro usr_mod_registro,
                       pt.fechamodificacionregistro fec_mod_registro
                from Puestotransfdistribucion pt
                 LEFT OUTER JOIN Unidadtransfdistribucion ut
                   ON pt.globalid = ut.puestotransfdistglobalid
                group by pt.alimentadorid, pt.objectid, round(pt.potenciakva,2), pt.usuarioregistro, pt.fecharegistro, pt.usuariomodificacionregistro, pt.fechamodificacionregistro
               )G
          where G.potenciakva <> G.potencianominalunidad
         )F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_PT_PotenciaUnidadyPuesto', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_PT_PotenciaUnidadyPuesto;

PROCEDURE P_V_CC_CodigoClienteRepetido IS
  BEGIN
    --Validación de Cuentas Repetidas

    Gv_DescERROR:= 'Cuentas Repetidas';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Conexion Consumidor',
           F.objectid,
           'CodigoCliente = ' || F.codigocliente,
           'ObjectIDPC = ' || F.ObjectidPC,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          SELECT p.alimentadorid,
                 GET_DESCRIPCION_ALIMENTADOR(p.alimentadorid) DescAlimentador,
                 c.objectid,
                 c.codigocliente,
                 z.usr_registro,
                 z.fec_registro,
                 z.usr_mod_registro,
                 z.fec_mod_registro,
                 p.objectid ObjectidPC
          FROM (
                SELECT x.CODIGOCLIENTE,
                       x.usuarioregistro              usr_registro,
                       x.fecharegistro                fec_registro,
                       x.usuariomodificacionregistro  usr_mod_registro,
                       x.fechamodificacionregistro    fec_mod_registro,
                       count(*)
                FROM conexionconsumidor x
                GROUP BY x.CODIGOCLIENTE, x.usuarioregistro, x.fecharegistro, x.usuariomodificacionregistro, x.fechamodificacionregistro
                HAVING COUNT(*) > 1 AND
                       x.CODIGOCLIENTE IS NOT NULL
               ) z
            INNER JOIN conexionconsumidor c
               ON z.codigocliente = c.codigocliente
            INNER JOIN PuntoCarga p
               ON c.puntocargaglobalid = p.globalid
            INNER JOIN atributosconsumidor a
               ON z.codigocliente = a.codigocliente
         ) F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_CC_CodigoClienteRepetido', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_CC_CodigoClienteRepetido;


PROCEDURE P_V_CC_CodigoUnicoRepetido IS
  BEGIN
    --Validación de Cuentas Repetidas

    Gv_DescERROR:= 'Cuentas Repetidas';

    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Conexion Consumidor',
           F.objectid,
           'CodigoUnico = ' || F.codigocliente,
           'ObjectIDPC = ' || F.ObjectidPC,
           TRUNC(sysdate),
           F.usr_registro,
           F.fec_registro,
           F.usr_mod_registro,
           F.fec_mod_registro
    FROM (
          SELECT p.alimentadorid,
                 GET_DESCRIPCION_ALIMENTADOR(p.alimentadorid) DescAlimentador,
                 c.objectid,
                 c.codigocliente,
                 z.usr_registro,
                 z.fec_registro,
                 z.usr_mod_registro,
                 z.fec_mod_registro,
                 p.objectid ObjectidPC
          FROM (
                SELECT x.CODIGOUNICO,
                       x.usuarioregistro              usr_registro,
                       x.fecharegistro                fec_registro,
                       x.usuariomodificacionregistro  usr_mod_registro,
                       x.fechamodificacionregistro    fec_mod_registro,
                       count(*)
                FROM conexionconsumidor x
                GROUP BY x.CODIGOUNICO, x.usuarioregistro, x.fecharegistro, x.usuariomodificacionregistro, x.fechamodificacionregistro
                HAVING COUNT(*) > 1 AND
                       x.CODIGOUNICO IS NOT NULL
               ) z
            INNER JOIN conexionconsumidor c
               ON z.codigounico = c.codigounico
            INNER JOIN PuntoCarga p
               ON c.puntocargaglobalid = p.globalid
            INNER JOIN atributosconsumidor a
               ON z.codigounico = a.codigounico
         ) F;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro)
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_CC_CodigoUnicoRepetido', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);
  END P_V_CC_CodigoUnicoRepetido;




  PROCEDURE P_V_CC_ConCodClienteSinMedidor IS
  BEGIN    
    --Validación de Conexion Consumidor con Código de Cliente y sin Número de Medidor
     
    Gv_DescERROR:= 'Con Código de Cliente y sin Número de Medidor en AtributosConsumidor';
  
    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro) 
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Conexion Consumidor',
           0,        
           'Existen ' || F.Cantidad || ' registros', 
           F.Cantidad2,
           TRUNC(sysdate),
           NULL,
           NULL,
           NULL,
           NULL
    FROM (
          select NULL alimentadorid,
                 NULL DescAlimentador,
                 0,
                 sum(G.Cantidad) Cantidad,
                 'Activos=' || sum(case when G.edccod = 'A' then G.Cantidad else 0 end) || '  ' ||
                 'Gestion Comercial=' || sum(case when G.edccod = '1' then G.Cantidad else 0 end) || '  ' ||
                 'Inactivo=' || sum(case when G.edccod = 'W' then G.Cantidad else 0 end) Cantidad2
          from (
                SELECT at.edccod,
                       count(*) Cantidad
                FROM conexionconsumidor cc 
                  inner join atributosconsumidor at
                    on cc.codigocliente = at.codigocliente 
                WHERE cc.codigocliente IS NOT NULL 
                  AND at.nummedidor IS NULL      
                group by at.edccod
               ) G 
         )F;

    COMMIT;
    
  EXCEPTION  
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro) 
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_CC_ConCodClienteSinMedidor', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);      
  END P_V_CC_ConCodClienteSinMedidor;
  
  
  PROCEDURE P_V_CC_SinCodClienteConMedidor IS
  BEGIN    
    --Validación de Conexion Consumidor sin Codigo de Cliente y con Número de Medidor
     
    Gv_DescERROR:= 'Sin Código de Cliente en Conexion Consumidor';
  
    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro) 
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Conexion Consumidor',
           0,        
           'Existen ' || F.Cantidad || ' registros', 
           NULL,
           TRUNC(sysdate),
           NULL,
           NULL,
           NULL,
           NULL
    FROM (
          SELECT NULL alimentadorid,
                 NULL DescAlimentador,
                 0,
                 count(*) Cantidad                 
          FROM conexionconsumidor cc
          WHERE cc.codigocliente IS NULL 
--            AND cc.mdenumfab IS NOT NULL                              
         ) F;

    COMMIT;
    
  EXCEPTION  
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro) 
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_CC_SinCodClienteConMedidor', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);      
  END P_V_CC_SinCodClienteConMedidor;
  

  PROCEDURE P_V_CC_SinCodClienteSinMedidor IS
  BEGIN    
    --Validación de Conexion Consumidor sin Codigo de Cliente y con Número de Medidor
     
    Gv_DescERROR:= 'Sin Código de Cliente y sin Número de Medidor en Conexion Consumidor';
  
    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro) 
    SELECT 'ADMS',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Conexion Consumidor',
           F.ObjectID,        
           'Existen ' || F.Cantidad || ' registros', 
           NULL,
           TRUNC(sysdate),
           NULL,
           NULL,
           NULL,
           NULL
    FROM (
          SELECT NULL alimentadorid,
                 NULL DescAlimentador,
                 ObjectID,
                 count(*) Cantidad                 
          FROM conexionconsumidor cc
          WHERE cc.codigocliente IS NULL 
          --  AND cc.mdenumfab IS NULL
         )F;

    COMMIT;
    
  EXCEPTION  
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro) 
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_CC_SinCodClienteSinMedidor', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);      
  END P_V_CC_SinCodClienteSinMedidor;
 
  PROCEDURE P_V_CC_SinCodUnicoSinNovedad IS
  BEGIN    
    Gv_DescERROR:= 'Conexion Consumidor no tiene CODIGOUNICO especificado y su NOVEDAD es [0]';
  
    INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro) 
    SELECT 'CIM',
           Gv_DescERROR,
           F.alimentadorid,
           F.DescAlimentador,
           'Conexion Consumidor',
           F.ObjectID,        
           'Existen ' || F.Cantidad || ' registros', 
           NULL,
           TRUNC(sysdate),
           NULL,
           NULL,
           NULL,
           NULL
    FROM (
          SELECT NULL alimentadorid,
                 NULL DescAlimentador,
                 ObjectID,
                 count(*) Cantidad                 
          FROM conexionconsumidor cc
          WHERE cc.codigounico IS NULL AND cc.novedades = 0
         )F;
    COMMIT;
    
  EXCEPTION  
    WHEN OTHERS THEN
      ROLLBACK;
      Gv_Error := 'ERROR: ' || SQLERRM;

      INSERT INTO VALIDAERROR(tipoerror, descerror, alimentadorid, descalimentador, elemento, objectid, datoadicional1, datoadicional2, fechavalidacion, usr_registro,  fec_registro, usr_mod_registro, fec_mod_registro) 
      VALUES('ERROR', Gv_Error, NULL, NULL, ' ', 0, 'Procedimiento: P_V_CC_SinCodUnicoSinNovedad', Gv_DescERROR, TRUNC(sysdate), NULL,NULL,NULL,NULL);      
  END P_V_CC_SinCodUnicoSinNovedad;

end PKG_RPT_ERRS;
/
