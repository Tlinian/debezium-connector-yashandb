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
}