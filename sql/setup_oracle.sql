create table datae.transacciones (
ID_TRANSACCION  NUMBER(8) NOT NULL, 
FECHA           DATE, 
ID_CLIENTE      NUMBER(8), 
MONTO           NUMBER(22,2),  
ID_PRODUCTO     NUMBER(8), 
ESTADO          VARCHAR2(1), 
COMENTARIO      VARCHAR2(4000),
CONSTRAINT PK_TRANSACCIONES PRIMARY KEY (ID_TRANSACCION));

DECLARE
    -- Definimos tipos para carga masiva
    TYPE t_trans IS TABLE OF DATAE.TRANSACCIONES%ROWTYPE;
    v_batch t_trans := t_trans();

    v_estados  SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST('P', 'C', 'F'); -- Pendiente, Completado, Fraude
    v_coments  SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST(
        'Excelente servicio al cliente', 
        'El producto llegó dañado y tarde', 
        'No reconozco este cargo en mi cuenta', 
        'Me encanta la rapidez del proceso', 
        'Problemas con el pago en la plataforma',
        'Transacción rutinaria mensual'
    );
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DATAE.TRANSACCIONES';

    FOR i IN 1 .. 50 -- Vamos a insertar 50 lotes de 10,000 cada uno (500k total)
    LOOP
        v_batch.DELETE;
        FOR j IN 1 .. 10000 
        LOOP
            v_batch.EXTEND;
            v_batch(v_batch.LAST).ID_TRANSACCION := ((i-1) * 10000) + j;
            v_batch(v_batch.LAST).FECHA          := SYSDATE - DBMS_RANDOM.VALUE(1, 365);
            v_batch(v_batch.LAST).ID_CLIENTE      := TRUNC(DBMS_RANDOM.VALUE(1000, 9999));
            v_batch(v_batch.LAST).MONTO           := ROUND(DBMS_RANDOM.VALUE(10, 5000), 2);
            v_batch(v_batch.LAST).ID_PRODUCTO     := TRUNC(DBMS_RANDOM.VALUE(1, 50));
            v_batch(v_batch.LAST).ESTADO          := v_estados(TRUNC(DBMS_RANDOM.VALUE(1, 4)));
            v_batch(v_batch.LAST).COMENTARIO      := v_coments(TRUNC(DBMS_RANDOM.VALUE(1, 7)));
        END LOOP;

        -- El FORALL es lo que da la velocidad de nivel Senior
        FORALL k IN 1 .. v_batch.COUNT
            INSERT INTO DATAE.TRANSACCIONES VALUES v_batch(k);
        COMMIT;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Carga masiva completada: 500,000 registros.');
END;