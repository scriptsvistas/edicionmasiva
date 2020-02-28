CREATE OR REPLACE PACKAGE PKG_VIEW_OTS AS 

  PROCEDURE P_A_Principal;
  /* TODO enter package declarations (types, exceptions, methods etc) here */ 
 
END PKG_VIEW_OTS;
/


CREATE OR REPLACE PACKAGE BODY PKG_VIEW_OTS AS

    CADENA      VARCHAR2(6000); 
    CURSOR FEATURES IS
     SELECT      allt.OWNER      USUARIO,
                 allt.TABLE_NAME NOM_FEAT
     FROM        all_tables      allt
     INNER JOIN  all_tab_columns allc        ON allt.table_name = allc.table_name
     INNER JOIN  sde.st_geometry_columns stc ON allt.table_name = stc.table_name
     WHERE       allt.OWNER       = 'SIGELEC'      AND  -- CUANDO EL USUARIO SEA SIGELEC 
                 ( NOT REGEXP_LIKE(allt.table_name, '^[A|D][0-9]') ) AND allc.COLUMN_NAME = 'ORDENTRABAJO'
      GROUP BY    allt.OWNER, allt.TABLE_NAME
      HAVING COUNT(*) = 1;

  PROCEDURE P_A_Principal AS
   
    BEGIN 
      
      FOR FS in FEATURES LOOP         
         CADENA := CADENA || ' SELECT ''' || FS.NOM_FEAT || ''' AS FEAT , NVL(MAX(ROWNUM),0) OTS FROM SIGELEC.' || FS.NOM_FEAT || ' WHERE ( ORDENTRABAJO IS NOT NULL ) UNION '; --  BULK COLLECT INTO FEAT_ROWS;                  
      END LOOP; 
      
      CADENA := SUBSTR( CADENA , 0 , LENGTH ( CADENA ) - 6 );
      CADENA := ' CREATE OR REPLACE VIEW V_ORDENES_SIG AS SELECT * FROM ( ' || CADENA || ' ) ';
      
      EXECUTE IMMEDIATE CADENA;
      
  END P_A_Principal;
END PKG_VIEW_OTS;
/
