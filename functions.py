from db import get_connection
import logging

def manutencao():
    script_indexe = """
    PRINT 'Iniciando otimizacao de indices...';

    -- Criando tabelas temporárias para armazenar os índices tratados
    IF OBJECT_ID('tempdb..#IndexesProcessed') IS NOT NULL DROP TABLE #IndexesProcessed;

    CREATE TABLE #IndexesProcessed (
        TableName NVARCHAR(128),
        IndexName NVARCHAR(128),
        Fragmentation DECIMAL(5,2),
        ActionTaken NVARCHAR(20), -- "REBUILD" ou "REORGANIZE"
        OptimizedAt DATETIME DEFAULT GETDATE()
    );

    DECLARE @TableName NVARCHAR(128);
    DECLARE @IndexName NVARCHAR(128);
    DECLARE @SQLIndex NVARCHAR(MAX);
    DECLARE @Fragmentation DECIMAL(5,2);
    DECLARE @UpdateStatsTable NVARCHAR(MAX) = '';

    DECLARE IndexCursor CURSOR FOR
    SELECT 
        OBJECT_NAME(i.object_id) AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'DETAILED') ips
    JOIN sys.indexes i 
        ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.index_id > 0
    ORDER BY ips.avg_fragmentation_in_percent DESC;

    OPEN IndexCursor;
    FETCH NEXT FROM IndexCursor INTO @TableName, @IndexName, @Fragmentation;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @Fragmentation >= 30
        BEGIN
            -- Realiza o REBUILD
            SET @SQLIndex = 'ALTER INDEX [' + @IndexName + '] ON [' + @TableName + '] REBUILD WITH (FILLFACTOR = 90);';
            EXEC sp_executesql @SQLIndex;

            PRINT 'Indice REBUILD: ' + @IndexName + ' na tabela: ' + @TableName;

            -- Registra o índice tratado
            INSERT INTO #IndexesProcessed (TableName, IndexName, Fragmentation, ActionTaken) 
            VALUES (@TableName, @IndexName, @Fragmentation, 'REBUILD');

            -- Adiciona a tabela à lista para atualização de estatísticas
            IF CHARINDEX(@TableName, @UpdateStatsTable) = 0
            BEGIN
                SET @UpdateStatsTable = @UpdateStatsTable + @TableName + ',';
            END
        END
        ELSE IF @Fragmentation >= 5 AND @Fragmentation < 30
        BEGIN
            -- Realiza o REORGANIZE
            SET @SQLIndex = 'ALTER INDEX [' + @IndexName + '] ON [' + @TableName + '] REORGANIZE;';
            EXEC sp_executesql @SQLIndex;

            PRINT 'Indice REORGANIZE: ' + @IndexName + ' na tabela: ' + @TableName;

            -- Registra o índice tratado
            INSERT INTO #IndexesProcessed (TableName, IndexName, Fragmentation, ActionTaken) 
            VALUES (@TableName, @IndexName, @Fragmentation, 'REORGANIZE');

            -- Adiciona a tabela à lista para atualização de estatísticas
            IF CHARINDEX(@TableName, @UpdateStatsTable) = 0
            BEGIN
                SET @UpdateStatsTable = @UpdateStatsTable + @TableName + ',';
            END
        END

        FETCH NEXT FROM IndexCursor INTO @TableName, @IndexName, @Fragmentation;
    END

    CLOSE IndexCursor;
    DEALLOCATE IndexCursor;

    -- Reorganizar os índices que foram rebuildados
    PRINT 'Reorganizando os indices que passaram por REBUILD...';

    DECLARE @RebuildTable NVARCHAR(128);
    DECLARE @RebuildIndex NVARCHAR(128);

    DECLARE RebuildCursor CURSOR FOR
    SELECT DISTINCT TableName, IndexName FROM #IndexesProcessed WHERE ActionTaken = 'REBUILD';

    OPEN RebuildCursor;
    FETCH NEXT FROM RebuildCursor INTO @RebuildTable, @RebuildIndex;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @SQLIndex = 'ALTER INDEX [' + @RebuildIndex + '] ON [' + @RebuildTable + '] REORGANIZE;';
        EXEC sp_executesql @SQLIndex;

        PRINT 'Indice REORGANIZE (pos-REBUILD): ' + @RebuildIndex + ' na tabela: ' + @RebuildTable;

        FETCH NEXT FROM RebuildCursor INTO @RebuildTable, @RebuildIndex;
    END

    CLOSE RebuildCursor;
    DEALLOCATE RebuildCursor;

    -- Atualizar estatísticas das tabelas que tiveram índices alterados
    IF LEN(@UpdateStatsTable) > 0
    BEGIN
        SET @UpdateStatsTable = LEFT(@UpdateStatsTable, LEN(@UpdateStatsTable) - 1);

        PRINT 'Atualizando estatisticas para as tabelas: ' + @UpdateStatsTable;

        DECLARE @CurrentTable NVARCHAR(128);
        DECLARE UpdateStatsCursor CURSOR FOR
        SELECT DISTINCT TableName FROM #IndexesProcessed;

        OPEN UpdateStatsCursor;
        FETCH NEXT FROM UpdateStatsCursor INTO @CurrentTable;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @SQLIndex = 'UPDATE STATISTICS [' + @CurrentTable + '] WITH FULLSCAN;';
            EXEC sp_executesql @SQLIndex;

            PRINT 'Estatisticas atualizadas na tabela: ' + @CurrentTable;

            FETCH NEXT FROM UpdateStatsCursor INTO @CurrentTable;
        END

        CLOSE UpdateStatsCursor;
        DEALLOCATE UpdateStatsCursor;
    END

    PRINT 'Otimizacao concluida!';

    """

    try:
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            print("🚀 Iniciando desfragmentação...")
            cursor.execute(script_indexe)

            while cursor.nextset():
                if cursor.messages:
                    for message in cursor.messages:
                        logging.info(f"SQL MESSAGE: {message[1]}")
                        print(f"SQL MESSAGE: {message[1]}")

            conn.commit()
            print("✅ Indexes atualizados!")
        else:
            print("❌ Falha ao conectar ao banco de dados.")
    except Exception as e:
        print(f"❌ Erro na atualização de indexes: {e}")
    finally:
        if conn:
            conn.close()



def cria_funcao():
    # Parte 1: Excluir a função se existir
    drop_function_script = """
    IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID('dbo.SplitString'))
    BEGIN
        DROP FUNCTION dbo.SplitString;
        PRINT 'Funcao SplitString excluida!';
    END
    """

    # Parte 2: Criar a função novamente
    create_function_script = """
    CREATE FUNCTION dbo.SplitString
    (
        @string NVARCHAR(MAX),
        @delimiter CHAR(1)
    )
    RETURNS @output TABLE (Item NVARCHAR(MAX))
    AS
    BEGIN
        DECLARE @start INT, @end INT;
        SET @start = 1;
        SET @end = CHARINDEX(@delimiter, @string);

        WHILE @end > 0
        BEGIN
            INSERT INTO @output (Item)
            VALUES (SUBSTRING(@string, @start, @end - @start));

            SET @start = @end + 1;
            SET @end = CHARINDEX(@delimiter, @string, @start);
        END

        -- Insere o último item da string
        INSERT INTO @output (Item)
        VALUES (SUBSTRING(@string, @start, LEN(@string) - @start + 1));

        RETURN;
    END
    """

    try:
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            
            # Executa a exclusão da função
            cursor.execute(drop_function_script)

            while cursor.nextset():
               if cursor.messages:
                   for message in cursor.messages:
                        logging.info(f"SQL MESSAGE: {message[1]}")
                        print(f"SQL MESSAGE: {message[1]}")

            conn.commit()

            # Executa a criação da função separadamente
            cursor.execute(create_function_script)
            conn.commit()

            print("✅ Função SplitString recriada com sucesso!")
        else:
            print("❌ Falha ao conectar ao banco de dados.")
    except Exception as e:
        print(f"❌ Erro na recriação da função: {e}")
    finally:
        if conn:
            conn.close()
