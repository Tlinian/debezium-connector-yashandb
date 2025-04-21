ALTER TABLE DDL_CREATE.branches DROP CONSTRAINT c_branches_1;

-- 存在外键时无法删除主键约束项
ALTER TABLE DDL_CREATE.department DROP PRIMARY KEY;
--YAS-02188 this unique/primary key is referenced by some foreign keys

-- 停用area表的主键约束，同时停用子表上的外键约束
ALTER TABLE DDL_CREATE.area MODIFY PRIMARY KEY DISABLE CASCADE;
ALTER TABLE DDL_CREATE.area MODIFY PRIMARY KEY ENABLE;

-- 在branches表上创建不启用的唯一约束，之后启用
ALTER TABLE DDL_CREATE.branches ADD UNIQUE(branch_no, area_no) DISABLE;
ALTER TABLE DDL_CREATE.branches MODIFY UNIQUE(branch_no, area_no) ENABLE;


-- 停用branches表的area_no外键约束
ALTER TABLE DDL_CREATE.branches ADD CONSTRAINT c_branches_1 
FOREIGN KEY (area_no) REFERENCES area(area_no) ON DELETE SET NULL;
ALTER TABLE DDL_CREATE.branches MODIFY CONSTRAINT c_branches_1 DISABLE;
-- 修改area_no为在area表中不存在的值
UPDATE branches SET area_no='99' WHERE area_no='01';
COMMIT;
-- 启用branches表的area_no外键约束，但不启用约束检查，则可以启用成功
ALTER TABLE DDL_CREATE.branches MODIFY CONSTRAINT c_branches_1 ENABLE NOVALIDATE;


-- modify_constraint中的如下例句：
ALTER TABLE DDL_CREATE.area MODIFY PRIMARY KEY DISABLE CASCADE;
ALTER TABLE DDL_CREATE.area MODIFY PRIMARY KEY ENABLE;
ALTER TABLE DDL_CREATE.branches MODIFY UNIQUE(branch_no, area_no) ENABLE;
ALTER TABLE DDL_CREATE.branches MODIFY CONSTRAINT c_branches_1 ENABLE NOVALIDATE;

-- 在enable_disable_constraint中可以如下表示：
ALTER TABLE DDL_CREATE.area DISABLE PRIMARY KEY CASCADE;
ALTER TABLE DDL_CREATE.area ENABLE PRIMARY KEY;
ALTER TABLE DDL_CREATE.branches ENABLE UNIQUE(branch_no, area_no);
ALTER TABLE DDL_CREATE.branches ENABLE NOVALIDATE CONSTRAINT c_branches_1;

-- 停用主键但保留对应索引
ALTER TABLE DDL_CREATE.area DISABLE PRIMARY KEY CASCADE KEEP INDEX;

-- 停用主键同时删除对应索引
ALTER TABLE DDL_CREATE.area DISABLE PRIMARY KEY CASCADE DROP INDEX;