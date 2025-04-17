package io.debezium.connector.yashandb.antlr;

import io.debezium.config.Configuration;
import io.debezium.connector.yashandb.YashanDBConnectorConfig;
import io.debezium.connector.yashandb.YashanDBValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import junit.framework.TestCase;
import org.assertj.core.api.Assertions;
import org.awaitility.core.AssertionCondition;
import org.junit.Test;

import java.util.Objects;

public class YashanDBDdlParserTest extends TestCase {

    public void testParse() {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null)
                , Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse("CREATE TABLE \"HSYS\".\"CUSTOMER_READING_ROUTE\"\n" +
                "(\"READING_ROUTE_ID\" BIGINT NOT NULL ENABLE,\n" +
                "\"REVISION\" INTEGER DEFAULT 0,\n" +
                "\"CREATED_BY\" BIGINT NOT NULL ENABLE,\n" +
                "\"CREATED_TIME\" TIMESTAMP(6) NOT NULL ENABLE,\n" +
                "\"UPDATED_BY\" BIGINT,\n" +
                "\"UPDATED_TIME\" TIMESTAMP(6),\n" +
                "\"DEPT_ID\" BIGINT NOT NULL ENABLE,\n" +
                "\"READING_ROUTE_CODE\" VARCHAR(60) NOT NULL ENABLE,\n" +
                "\"READING_ROUTE_NAME\" VARCHAR(150),\n" +
                "\"REMARK\" VARCHAR(768),\n" +
                "CONSTRAINT \"SYS_C_19536\" PRIMARY KEY (\"READING_ROUTE_ID\")\n" +
                "USING INDEX\n" +
                "PCTFREE 8 INITRANS 2 MAXTRANS 255\n" +
                "TABLESPACE \"USERS\" ENABLE\n" +
                ");", databaseTables);
        ddlParser.parse("alter table \"HSYS\".\"CUSTOMER_READING_ROUTE\" add column id01 NUMBER;", databaseTables);
        Table table = databaseTables.forTable(new TableId(null, "HSYS", "CUSTOMER_READING_ROUTE"));
        Column id01 = table.columnWithName("ID01");
        assert Objects.equals(id01.typeName(), "NUMBER");


        ddlParser.parse("CREATE TABLE \"HSYS\".\"CUSTOMER_READING_ROUTE1111\"\n" +
                "(\"READING_ROUTE_ID\" BIGINT NOT NULL ENABLE,\n" +
                "\"REVISION\" INTEGER DEFAULT 0,\n" +
                "\"CREATED_BY\" BIGINT NOT NULL ENABLE,\n" +
                "\"CREATED_TIME\" TIMESTAMP(6) NOT NULL ENABLE,\n" +
                "\"UPDATED_BY\" BIGINT,\n" +
                "\"UPDATED_TIME\" TIMESTAMP(6),\n" +
                "\"DEPT_ID\" BIGINT NOT NULL ENABLE,\n" +
                "\"READING_ROUTE_CODE\" NVARCHAR(40) NOT NULL ENABLE,\n" +
                "\"READING_ROUTE_NAME\" NVARCHAR(100),\n" +
                "\"REMARK\" NVARCHAR(512),\n" +
                "PRIMARY KEY (\"READING_ROUTE_ID\")\n" +
                "USING INDEX\n" +
                "PCTFREE 8 INITRANS 2 MAXTRANS 255\n" +
                "TABLESPACE \"USERS\" ENABLE\n" +
                ") PCTFREE 8 INITRANS 2 MAXTRANS 255\n" +
                "LOGGING\n" +
                "TABLESPACE \"USERS\"\n" +
                "SEGMENT CREATION DEFERRED\n" +
                "ORGANIZATION HEAP;", databaseTables);
        ddlParser.parse("ALTER TABLE HSYS.CUSTOMER_READING_ROUTE1111 MODIFY READING_ROUTE_CODE NVARCHAR(10);", databaseTables);
        ddlParser.parse("ALTER TABLE HSYS.CUSTOMER_READING_ROUTE1111 MODIFY REMARK NVARCHAR(512);", databaseTables);
        ddlParser.parse("ALTER TABLE HSYS.CUSTOMER_READING_ROUTE1111 MODIFY READING_ROUTE_NAME NVARCHAR(100);", databaseTables);
        assert databaseTables.forTable(new TableId(null, "HSYS", "CUSTOMER_READING_ROUTE1111")) != null;
        assert Objects.equals(databaseTables.forTable(new TableId(null, "HSYS", "CUSTOMER_READING_ROUTE1111"))
                .columnWithName("READING_ROUTE_CODE").typeName(), "NVARCHAR2");
    }

    @Test
    public void testTime() {
        String sql = "create table TEST_TIME_DB.TIME_TAB01(id int,id01 time)";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null)
                , Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println();
    }

    @Test
    public void testTableNameIsEnd() {
        String sql = "create table TEST_TIME_DB.END(id int,id01 time)";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null)
                , Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println();
    }

    @Test
    public void testDdl() {
        String sql = "create table KAFKA_DDL.tab(id int) ORGANIZATION external (type YASDB_LOADER  access parameters (RECORDS DELIMITED BY NEWLINE));";
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null)
                , Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        ddlParser.parse(sql, databaseTables);
        System.out.println(databaseTables);
    }

    public void testExternalTable() {
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null)
                , Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // access_driver_type
        String sql = "create table KAFKA_DDL.location_specifier(id int) ORGANIZATION\n" +
                " EXTERNAL (TYPE YASDB_LOADER) ";
        ddlParser.parse(sql, databaseTables);
        String sql2 = "create table KAFKA_DDL.location_specifier2(id int) ORGANIZATION\n" +
                " EXTERNAL (TYPE ORACLE_LOADER) ";

        ddlParser.parse(sql2, databaseTables);
        // external_table_data_props_clause
        // record_format_info_clause
        String sql3 = "create table KAFKA_DDL.record_format_info_clause1(id int) ORGANIZATION external" +
                " (  access parameters (RECORDS DELIMITED BY NEWLINE));";
        ddlParser.parse(sql3, databaseTables);
        // output_files_clause
        String sql13 = "create table KAFKA_DDL.record_format_info_clause2(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS NOBADFILE))";
        ddlParser.parse(sql13, databaseTables);

        String sql23 = "create table KAFKA_DDL.record_format_info_clause3(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS BADFILE test:'./'));";
        ddlParser.parse(sql23, databaseTables);

        String sql33 = "create table KAFKA_DDL.record_format_info_clause4(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS NOLOGFILE));";
        ddlParser.parse(sql33, databaseTables);

        String sql43 = "create table KAFKA_DDL.record_format_info_clause5(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS LOGFILE test:'./'));";
        ddlParser.parse(sql43, databaseTables);
        // field_definitions_clause
        String sql4 = "create table KAFKA_DDL.tab12(id int) ORGANIZATION " +
                "external (type YASDB_LOADER default directory test access parameters (RECORDS DELIMITED BY NEWLINE));";

        ddlParser.parse(sql4, databaseTables);

        String sql14 = "create table KAFKA_DDL.field_definitions_clause1(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS TERMINATED BY 'char'));";

        ddlParser.parse(sql14, databaseTables);

        String sql24 = "create table KAFKA_DDL.field_definitions_clause2(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS OPTIONALLY ENCLOSED BY 'char'));";

        ddlParser.parse(sql24, databaseTables);

        String sql34 = "create table KAFKA_DDL.field_definitions_clause3(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS));";

        ddlParser.parse(sql34, databaseTables);

        String sql44 = "create table KAFKA_DDL.field_definitions_clause4(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS TERMINATED BY 0x'9'));";

        ddlParser.parse(sql44, databaseTables);

        String sql54 = "create table KAFKA_DDL.field_definitions_clause5(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS TERMINATED BY x'9'));";

        ddlParser.parse(sql54, databaseTables);
        // LOCATION
        String sql542 = "create table KAFKA_DDL.location_specifier(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS TERMINATED BY x'9') LOCATION (test:'./test.csv'));";

        ddlParser.parse(sql542, databaseTables);
        // REJECT
        String reject1 = "create table KAFKA_DDL.REJECT1(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS TERMINATED BY x'9') LOCATION (test:'./test.csv')) REJECT LIMIT 1;";

        ddlParser.parse(reject1, databaseTables);

        String reject2 = "create table KAFKA_DDL.REJECT1(id int) ORGANIZATION\n" +
                "    external (type YASDB_LOADER default directory test access parameters\n" +
                "    (RECORDS FIELDS TERMINATED BY x'9') LOCATION (test:'./test.csv')) REJECT LIMIT UNLIMITED;";

        ddlParser.parse(reject2, databaseTables);
        System.out.println(databaseTables);
    }

    public void testLscProperties(){
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null)
                , Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // access_driver_type
        String sql = "      CREATE TABLE finance_info\n" +
                "(year CHAR(4) ,\n" +
                "month CHAR(2) ,\n" +
                "branch CHAR(4) ,\n" +
                "revenue_total NUMBER(10,2),\n" +
                "cost_total NUMBER(10,2),\n" +
                "fee_total NUMBER(10,2)\n" +
                ") ORGANIZATION LSC\n" +
                "COMPRESSION lz4 HIGH\n" +
                "ORDER BY (year,month,branch)\n" +
                "MCOL TTL '1' MONTH;";
        ddlParser.parse(sql, databaseTables);
        System.out.println(databaseTables);
    }

    public void testTableType(){
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null)
                , Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table type
        String sql = "create global temporary table tem_tab(id int );";
        ddlParser.parse(sql, databaseTables);
        String sql1 = "CREATE SHARDED TABLE area_shard\n" +
                "(area_no CHAR(2) NOT NULL,\n" +
                "area_name VARCHAR2(60),\n" +
                "DHQ VARCHAR2(20) DEFAULT 'ShenZhen' NOT NULL);";
        ddlParser.parse(sql1, databaseTables);
        String sql2 = "     CREATE DUPLICATED TABLE area_dupli\n" +
                "(area_no CHAR(2) NOT NULL,\n" +
                "area_name VARCHAR2(60),\n" +
                "DHQ VARCHAR2(20) DEFAULT 'ShenZhen' NOT NULL);";
        ddlParser.parse(sql2, databaseTables);
        System.out.println(databaseTables);
    }

    public void testTempTableAttrClause(){
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null)
                , Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table_properties temp_table_attr_clause
        String sql = "create table attr_tab(id int) on commit  drop definition;";
        ddlParser.parse(sql, databaseTables);
        String sql1 = "create table attr_tab2(id int) on commit preserve definition;";
        ddlParser.parse(sql1, databaseTables);
        String sql2 = "create table attr_tab3(id int) on commit preserve rows;";
        ddlParser.parse(sql2, databaseTables);
        String sql3 = "create table attr_tab4(id int) on commit delete rows;";
        ddlParser.parse(sql3, databaseTables);
        System.out.println(databaseTables);
    }

    public void testOrganizationClause(){
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null)
                , Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table_properties organization_clause
        String sql = "create table organ_heap(id int)organization heap;";
        ddlParser.parse(sql, databaseTables);
        String sql1 = "create table organ_heap1(id int)organization tac;";
        ddlParser.parse(sql1, databaseTables);
        String sql2 = "create table organ_heap2(id int)organization lsc;";
        ddlParser.parse(sql2, databaseTables);
        System.out.println(databaseTables);
    }

    public void testExternalTableClause(){
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null)
                , Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table_properties external_table_clause
        String sql = "create table organ_heap(id int)organization EXTERNAL (type YASDB_LOADER);";
        ddlParser.parse(sql, databaseTables);
        String sql1 = "create table organ_heap2(id int)organization EXTERNAL (type ORACLE_LOADER);";
        ddlParser.parse(sql1, databaseTables);
        System.out.println(databaseTables);
    }

    public void testPhysicalAttributeClause(){
        YashanDBDdlParser ddlParser = new YashanDBDdlParser(false, new YashanDBValueConverters(new YashanDBConnectorConfig(Configuration.create().build()),
                null)
                , Tables.TableFilter.includeAll());
        Tables databaseTables = new Tables();
        // table_properties physical_attribute_clause
        String sql = "create table physical_attribute_tab(id int)tablespace users;";
        ddlParser.parse(sql, databaseTables);
        String sql1 = "create table physical_attribute_tab1(id int)tablespace set users;";
        ddlParser.parse(sql1, databaseTables);
        String sql2 = "create table physical_attribute_tab2(id int)PCTFREE 1;";
        ddlParser.parse(sql2, databaseTables);
        String sql3 = "create table physical_attribute_tab3(id int)PCTUSED 1;";
        ddlParser.parse(sql3, databaseTables);

        String sql4 = "create table physical_attribute_tab4(id int)INITRANS 1;";
        ddlParser.parse(sql4, databaseTables);
        String sql5 = "create table physical_attribute_tab5(id int)MAXTRANS 1;";
        ddlParser.parse(sql5, databaseTables);
        String sql6 = "create table physical_attribute_tab15(id int)segment creation immediate;";
        ddlParser.parse(sql6, databaseTables);

        String sql7 = "create table physical_attribute_tab25(id int)segment creation deferred;";
        ddlParser.parse(sql7, databaseTables);
        String sql8 = "CREATE TABLE part_storage1(a INT, b VARCHAR(4000))\n" +
                "PARTITION BY RANGE(a, b)\n" +
                "(\n" +
                "\tPARTITION p1 VALUES LESS THAN(1, 'a') STORAGE(INITIAL 0 MAXSIZE 1M NEXT 0),\n" +
                "\tPARTITION p2 VALUES LESS THAN(10, 'c') STORAGE(MINEXTENTS 1 MAXEXTENTS 10 PCTINCREASE 0),\n" +
                "\tPARTITION p3 VALUES LESS THAN(MAXVALUE, MAXVALUE) STORAGE(INITIAL 0 MAXSIZE 1M NEXT 0 MINEXTENTS 1 MAXEXTENTS 10 PCTINCREASE 0 FREELIST GROUPS 20 BUFFER_POOL RECYCLE FLASH_CACHE KEEP CELL_FLASH_CACHE DEFAULT)\n" +
                ")\n" +
                "STORAGE(INITIAL 63K MAXSIZE 10M NEXT 12k MINEXTENTS 1 MAXEXTENTS 10 PCTINCREASE 0 FREELISTS 10);";
        ddlParser.parse(sql8, databaseTables);
        System.out.println(databaseTables);
    }
}