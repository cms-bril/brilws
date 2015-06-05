/* tablelist: IOVP_BOOLEAN,IOVP_STRING,IOVTAGDATA,IOVP_BLOB,IOVP_FLOAT,IOVP_INT,IOVP_SMALLINT,IOVTAGS */
CREATE TABLE IOVP_BOOLEAN(PAYLOADID NUMBER(20) ,IFIELD NUMBER(10) ,VAL NUMBER(1) ,CONSTRAINT IOVP_BOOLEAN_PK PRIMARY KEY(PAYLOADID,IFIELD));
GRANT SELECT ON "IOVP_BOOLEAN" TO PUBLIC;

CREATE TABLE IOVP_STRING(PAYLOADID NUMBER(20) ,IFIELD NUMBER(10) ,VAL VARCHAR2(4000) ,CONSTRAINT IOVP_STRING_PK PRIMARY KEY(PAYLOADID,IFIELD));
GRANT SELECT ON "IOVP_STRING" TO PUBLIC;

CREATE TABLE IOVTAGDATA(TAGID NUMBER(20) ,SINCE NUMBER(10) ,PAYLOADDICT VARCHAR2(4000) NOT NULL ,PAYLOADID NUMBER(20) NOT NULL ,FUNC VARCHAR2(4000) ,COMMENTS VARCHAR2(4000) ,CONSTRAINT IOVTAGDATA_PK PRIMARY KEY(TAGID,SINCE));
GRANT SELECT ON "IOVTAGDATA" TO PUBLIC;

CREATE TABLE IOVP_BLOB(PAYLOADID NUMBER(20) ,IFIELD NUMBER(10) ,VAL BLOB ,CONSTRAINT IOVP_BLOB_PK PRIMARY KEY(PAYLOADID,IFIELD));
GRANT SELECT ON "IOVP_BLOB" TO PUBLIC;

CREATE TABLE IOVP_FLOAT(PAYLOADID NUMBER(20) ,IFIELD NUMBER(10) ,VAL BINARY_FLOAT ,CONSTRAINT IOVP_FLOAT_PK PRIMARY KEY(PAYLOADID,IFIELD));
GRANT SELECT ON "IOVP_FLOAT" TO PUBLIC;

CREATE TABLE IOVP_INT(PAYLOADID NUMBER(20) ,IFIELD NUMBER(10) ,VAL NUMBER(10) ,CONSTRAINT IOVP_INT_PK PRIMARY KEY(PAYLOADID,IFIELD));
GRANT SELECT ON "IOVP_INT" TO PUBLIC;

CREATE TABLE IOVP_SMALLINT(PAYLOADID NUMBER(20) ,IFIELD NUMBER(10) ,VAL NUMBER(5) ,CONSTRAINT IOVP_SMALLINT_PK PRIMARY KEY(PAYLOADID,IFIELD));
GRANT SELECT ON "IOVP_SMALLINT" TO PUBLIC;

CREATE TABLE IOVTAGS(TAGID NUMBER(20) ,TAGNAME VARCHAR2(4000) ,CREATIONUTC VARCHAR2(4000) ,APPLYTO VARCHAR2(4000) ,DATASOURCE VARCHAR2(4000) ,ISDEFAULT NUMBER(1) ,COMMENTS VARCHAR2(4000) ,CONSTRAINT IOVTAGS_PK PRIMARY KEY(TAGID),CONSTRAINT IOVTAGS_UQ UNIQUE (TAGNAME));
GRANT SELECT ON "IOVTAGS" TO PUBLIC;
