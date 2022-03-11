
CREATE LIBRARY "VT_TOOLS" LANGUAGE SQLSCRIPT AS
BEGIN
    PRIVATE procedure validate_datalake as begin
        declare tcount integer;

        -- Ensure we have access to the data lake table definitions.

        -- Check to see if we can already see the data lake system table
        -- that contains all the tables and columns in the lake.

        select count(1) into tcount
          from tables 
         where table_name = 'DL_SYSCOLUMNS_VT';
         
        if tcount = 0 then
            -- Create a virtual table on the SYSCOLUMNS "system view" from the data lake.
            -- This gives us access to the tables and columns that exist in the data lake.
            
            dwc_global.data_lake_create_virtual_table ( 
                virtual_table_name   => 'DL_SYSCOLUMNS_VT',
                data_lake_table_name => 'SYSCOLUMNS',
                target_schema_sys    => TRUE) ;
        end if;
    end;
    
    PUBLIC procedure drop_datalake_table(p_table_name nvarchar(200)) as begin
        declare tCount integer;

        validate_datalake();
        
        execute immediate 'select count(distinct "tname") from dl_syscolumns_vt where "tname" = :p_table_name'
           into tCount using p_table_name;

        if tcount = 1 then
            call dwc_global.data_lake_execute('drop table "' || p_table_name || '"');
        end if;
    end;
  PUBLIC procedure drop_virtual_table(p_table_name    nvarchar(200),
                                        p_drop_datalake boolean default true) as begin
        declare v_count integer;
        declare v_remote_object_name nvarchar(200) = null;
        
        -- See if the requested virtual table exists.  Getting
        -- a row in this view means the data lake is present
        -- and this is a valid virtual table.
        
        select remote_object_name
          into v_remote_object_name
          from virtual_tables
         where table_name = p_table_name;

        if v_remote_object_name is not null then
            -- drop the virtural table first in HANA.
            execute immediate 'drop table ' || p_table_name;

            -- Clean up the data lake table - if requested.
            
            if p_drop_datalake = true then
                drop_datalake_table(v_remote_object_name);
            end if;
        end if;
    end;
  PUBLIC procedure ensure_table_in_datalake(p_table_name nvarchar(200),
                                              p_force_lake boolean default false) as begin
        using sqlscript_print as print;
        
        declare datalake_table_exists condition for sql_error_code 10000;
        
        declare datalake_sql   clob = 'create table "' || p_table_name || '" (';
        declare tmp_table_name nvarchar(200) = p_table_name || '_' || SYSUUID;

        declare comma        nvarchar(1) = '';
        declare tcount       integer = 1;
        declare temp_sql     nvarchar(4000);
       
        declare cursor table_columns_cursor for
            select * from table_columns
             where table_name = p_table_name
             order by position;

        -- See if the requested table is present and is not already a virtual table.
        
        select count(1) into tcount
          from tables
         where table_name = p_table_name
           and table_type in ('ROW', 'COLUMN' /* does not include 'VIRTUAL' */);
        
        if tcount = 1 then
            -- We do have a normal HANA table having the target name.
            
            validate_datalake();
    
            -- See if the table already exists in the data lake - this is an invalid situation.
            
            -- NOTE: this test is dynamic SQL so it can be compiled if the data 
            -- lake table has not yet been created as a virtual table.
            
            temp_sql = 'select count(distinct "tname") from dl_syscolumns_vt where "tname" = ?';
            execute immediate temp_sql into tcount using p_table_name;
            
            if tcount = 1 then
                print:print_line('here');
                
                if p_force_lake = true then
                    -- We have been told to drop the virtual table if it already exists in the data lake.
                    
                    drop_datalake_table(p_table_name);
                else
                    -- This is an error condition - signal an exception.
                    
                    signal datalake_table_exists set message_text = 'Data lake table ' || p_table_name || ' already exists.  Please use p_force_create=true to avoid this error.';
                end if;
            end if;

            -- Build the data lake create statement based on the HANA table.
            
            FOR table_column AS table_columns_cursor DO
                datalake_sql = datalake_sql || CHAR(10) || comma;
                datalake_sql = datalake_sql || '"' || table_column.column_name || '" ';
               
                IF table_column.data_type_name = 'NVARCHAR' THEN
                    datalake_sql = datalake_sql || 'varchar';
                    datalake_sql = datalake_sql || '(' || table_column.LENGTH * 3 || ')';
                ELSEIF table_column.data_type_name = 'SECONDDATE' THEN
                    datalake_sql = datalake_sql || 'timestamp';
                ELSEIF table_column.data_type_name = 'SMALLDECIMAL' THEN
                    datalake_sql = datalake_sql || 'decimal';
                ELSEIF table_column.data_type_name = 'NCLOB' THEN
                    datalake_sql = datalake_sql || 'CLOB';
                ELSEIF table_column.data_type_name = 'DECIMAL' THEN
                    if table_column.length = 34 and table_column.scale is null THEN
                        datalake_sql = datalake_sql || table_column.data_type_name;
                    else
                        datalake_sql = datalake_sql || 'DECIMAL(' || table_column.length || ',' || table_column.scale || ')';
                    end if;
                ELSEIF table_column.data_type_name = 'FLOAT' THEN
                    if table_column.length = 34 and table_column.scale is null THEN
                        datalake_sql = datalake_sql || table_column.data_type_name;
                    else
                        datalake_sql = datalake_sql || 'DECIMAL(' || table_column.length || ',' || table_column.scale || ')';
                    end if;
                ELSE
                    datalake_sql = datalake_sql || table_column.data_type_name;
                END IF;
                
                if table_column.is_nullable = 'FALSE' then
                    datalake_sql = datalake_sql || ' NOT NULL';
                end if;
            
                comma = ',';
            end for;

            datalake_sql = datalake_sql || ')';
            print:print_line(datalake_sql);

            dwc_global.data_lake_execute(datalake_sql);
            
            -- Move the original table to a hold name so we can
            -- replace it with the virtural table.

            execute immediate 'RENAME TABLE "' || p_table_name || '" TO "' || tmp_table_name || '"';
            
            dwc_global.data_lake_create_virtual_table (
                virtual_table_name => p_table_name,
                data_lake_table_name => p_table_name);
                
            execute immediate 'INSERT INTO ' || p_table_name || ' SELECT * FROM ' || tmp_table_name;
            COMMIT;

            execute immediate 'DROP TABLE "' || tmp_table_name || '"';
        end if;
    END;
END
