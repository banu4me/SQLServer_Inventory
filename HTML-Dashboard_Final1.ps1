# Define SQL Servers
$servers = get-content -path "G:\Healthcheck-report\serverlist.txt"

# Define SQL query to collect metrics
$uptimequery = @"
SELECT 
    sqlserver_start_time AS 'SQL Server Start Time',
    DATEDIFF(SECOND, sqlserver_start_time, GETDATE()) / 86400 AS 'Days',
    (DATEDIFF(SECOND, sqlserver_start_time, GETDATE()) % 86400) / 3600 AS 'Hours',
    ((DATEDIFF(SECOND, sqlserver_start_time, GETDATE()) % 86400) % 3600) / 60 AS 'Minutes',
    ((DATEDIFF(SECOND, sqlserver_start_time, GETDATE()) % 86400) % 3600) % 60 AS 'Seconds',
    DATEDIFF(SECOND, sqlserver_start_time, GETDATE()) AS 'Total Uptime (Seconds)'
FROM sys.dm_os_sys_info;
"@
 
$query = @"
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY SERVERPROPERTY('servername')) AS 'SNO',
    SERVERPROPERTY('servername') AS ServerName,
    CASE 
        WHEN SERVERPROPERTY('servername') ='RACK' THEN 'PROD' 
        WHEN SERVERPROPERTY('servername') ='RACK58' THEN 'PROD' 
        WHEN SERVERPROPERTY('servername') ='RACK2' THEN 'PROD' 
        WHEN SERVERPROPERTY('servername') ='RACK4' THEN 'PROD' 
        WHEN SERVERPROPERTY('servername') ='RACK8' THEN 'Archival' 
        WHEN SERVERPROPERTY('servername') ='RACK9' THEN 'STAGING' 
        WHEN SERVERPROPERTY('servername') ='DCA_UAT' THEN 'UAT' 
        WHEN SERVERPROPERTY('servername') ='RACK13' THEN 'STAGING' 
        WHEN SERVERPROPERTY('servername') ='RACK30' THEN 'STAGING' 
        WHEN SERVERPROPERTY('servername') ='RACK34' THEN 'PROD' 
        WHEN SERVERPROPERTY('servername') ='SERVER' THEN 'Domain' 
        WHEN SERVERPROPERTY('servername') ='RACK31' THEN 'PROD'
        WHEN SERVERPROPERTY('servername') ='RACK42' THEN 'STAGING'
        WHEN SERVERPROPERTY('servername') ='RACK53' THEN 'STAGING'
        WHEN SERVERPROPERTY('servername') ='RACK40' THEN 'STAGING'
        WHEN SERVERPROPERTY('servername') ='BULK_CATS' THEN 'STAGING'
        WHEN SERVERPROPERTY('servername') ='RACK65' THEN 'PROD'
        WHEN SERVERPROPERTY('servername') ='RACK40' THEN 'Development'
        WHEN SERVERPROPERTY('servername') ='RACK64' THEN 'TESTING'
        WHEN SERVERPROPERTY('servername') ='RACK72' THEN 'PROD'
    END AS Env,
    (SELECT 'Online') AS Status,
    LEFT (@@VERSION, 37) AS SQL_Version,
    SERVERPROPERTY('ProductVersion') AS SQLVersion,
    SERVERPROPERTY('ProductLevel') AS ProductLevel,
    SERVERPROPERTY('Edition') AS Edition,
    (SELECT COUNT(*) FROM sys.databases WHERE state = 0) AS OnlineDatabases,
    (SELECT SUM((mFiles.size) * 8 / 1024) / 1024 FROM SYS.MASTER_FILES mFiles 
     INNER JOIN SYS.DATABASES dbs ON dbs.DATABASE_ID = mFiles.DATABASE_ID) AS Tot_DB_Size_GB,
    (SELECT COUNT(*) FROM sys.dm_exec_requests) AS ActiveRequests,
    (SELECT cpu_count FROM sys.dm_os_sys_info) AS No_Of_Processors,
    (SELECT FORMAT((physical_memory_kb / 1024.00 / 1024.00), 'N2') FROM sys.dm_os_sys_info) AS TotPhysicalMemory,
    (SELECT c.value FROM sys.configurations c WHERE c.[name] = 'max server memory (MB)') AS SQL_Memory
"@

# Define SQL query to collect backup report
$backupQuery = @"
SELECT  @@servername as Server_name, name AS DBName ,
                recovery_model_desc AS RecoveryModel ,
                state_desc AS DBStatus ,
                d AS 'LastFullBackup' ,
                i AS 'LastDiffBackup' ,
                l AS 'LastLogBackup'
        FROM    ( SELECT    db.name ,
                            db.state_desc ,
                            db.recovery_model_desc ,
                            type ,
                            backup_finish_date
                  FROM      master.sys.databases db
                            LEFT OUTER JOIN msdb.dbo.backupset a ON a.database_name = db.name
                ) AS Sourcetable 
            PIVOT 
                ( MAX(backup_finish_date) FOR type IN ( D, I, L ) ) AS MostRecentBackup where name<>'tempdb' order by Server_name, RecoveryModel
"@

#CPU utilization
$cpu = @"
--> STEP 1 : 

	--CPU Utilisation

	SET NOCOUNT ON
	DECLARE @ts_now bigint 
	DECLARE @enable Varchar (60);
	DECLARE @AvgCPUUtilization DECIMAL(10,2) 

	SELECT @ts_now = cpu_ticks/(cpu_ticks/ms_ticks) FROM sys.dm_os_sys_info

	-- load the CPU utilization in the past 10 minutes into the temp table, you can load them into a permanent table
	SELECT TOP(10) SQLProcessUtilization AS [SQLServerProcessCPUUtilization]
	,SystemIdle AS [SystemIdleProcess]
	,100 - SystemIdle - SQLProcessUtilization AS [OtherProcessCPU Utilization]
	,DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) AS [EventTime] 
	INTO #CPUUtilization
	FROM ( 
			SELECT record.value('(./Record/@id)[1]', 'int') AS record_id, 
				record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') 
				AS [SystemIdle], 
				record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 
				'int') 
				AS [SQLProcessUtilization], [timestamp] 
			FROM ( 
				SELECT [timestamp], CONVERT(xml, record) AS [record] 
				FROM sys.dm_os_ring_buffers 
				WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
				AND record LIKE '%<SystemHealth>%') AS x 
			) AS y 
	ORDER BY record_id DESC
	--select * from #CPUUtilization
	


--> STEP 2 : 

	DECLARE @xp_msver TABLE 
	(
		[idx] [int] NULL
		,[c_name] [varchar](100) NULL
		,[int_val] [float] NULL
		,[c_val] [varchar](128) NULL
	)
 
	INSERT INTO @xp_msver
	EXEC ('[master]..[xp_msver]');;
 
	WITH [ProcessorInfo] AS 
	(
		SELECT ([cpu_count] / [hyperthread_ratio]) AS [number_of_physical_cpus]
			,CASE
				WHEN hyperthread_ratio = cpu_count
					THEN cpu_count
				ELSE (([cpu_count] - [hyperthread_ratio]) / ([cpu_count] / [hyperthread_ratio]))
				END AS [number_of_cores_per_cpu]
			,CASE
				WHEN hyperthread_ratio = cpu_count
					THEN cpu_count
				ELSE ([cpu_count] / [hyperthread_ratio]) * (([cpu_count] - [hyperthread_ratio]) / ([cpu_count] / [hyperthread_ratio]))
				END AS [total_number_of_cores]
			,[cpu_count] AS [number_of_virtual_cpus]
			,(
				SELECT [c_val]
				FROM @xp_msver
				WHERE [c_name] = 'Platform'
				) AS [cpu_category]
		FROM [sys].[dm_os_sys_info]
	),
	[ProcessorInfo1] as 
	(
		SELECT @@SERVERNAME servername,[number_of_physical_cpus]
			,[number_of_cores_per_cpu]
			,[total_number_of_cores]
			,[number_of_virtual_cpus]
			,LTRIM(RIGHT([cpu_category], CHARINDEX('x', [cpu_category]) - 1)) AS [cpu_category]
		FROM [ProcessorInfo]
	),
	CPUUtilization as
	(
		select @@SERVERNAME [ServerName],avg(SQLServerProcessCPUUtilization)[AVG_Utilization],
		case 
			when avg(SQLServerProcessCPUUtilization) > = 80 then 'CPU Utilisation is High' 
			else 'CPU Utilisation looks Good'
		end[Status]
		from #CPUUtilization
	) 

	select A.servername,	number_of_physical_cpus,	number_of_cores_per_cpu,	total_number_of_cores,	number_of_virtual_cpus,	cpu_category,B.[AVG_Utilization],B.[Status] 
	from [ProcessorInfo1] a
	left join CPUUtilization b on a.servername = b.ServerName

--> STEP 3 :  DROP TABLE 

	DROP TABLE #CPUUtilization
"@


#Disk Information
$diskquery = @"
-- Create a temporary table to store the disk space information
CREATE TABLE #DiskSpaceInfo (
    Server_Name NVARCHAR(128),
    DiskMountPoint NVARCHAR(128),
    TotalSizeGB DECIMAL(18, 2),
    AvailableSizeGB DECIMAL(18, 2),
    PercentageFree DECIMAL(5, 2),
    Status NVARCHAR(20)
);

-- Insert disk space information into the temporary table
INSERT INTO #DiskSpaceInfo
SELECT
    @@SERVERNAME AS Server_Name,
    vs.volume_mount_point AS DiskMountPoint,
    vs.total_bytes / 1073741824.0 AS TotalSizeGB,
    vs.available_bytes / 1073741824.0 AS AvailableSizeGB,
    ROUND((vs.available_bytes * 100.0 / vs.total_bytes), 2) AS PercentageFree,
    CASE 
        WHEN vs.available_bytes * 100.0 / vs.total_bytes < 10 THEN 'Critical'
        WHEN vs.available_bytes * 100.0 / vs.total_bytes < 20 THEN 'Warning'
        ELSE 'Healthy'
    END AS Status
FROM
    sys.master_files AS mf
CROSS APPLY
    sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS vs
GROUP BY
    vs.volume_mount_point,
    vs.total_bytes,
    vs.available_bytes;

-- Select the disk space information
SELECT 
    Server_Name,
    DiskMountPoint,
    TotalSizeGB,
    AvailableSizeGB,
    PercentageFree AS [SpaceFree],
    Status
FROM 
    #DiskSpaceInfo
ORDER BY 
    DiskMountPoint;

-- Drop the temporary table
DROP TABLE #DiskSpaceInfo;
"@


#SQL Server Service Status Information
$servicequery = @"
SELECT @@servername Server_Name, DSS.servicename as ServiceName,
    DSS.startup_type_desc as StartUpType,
    DSS.status_desc as Status,
    DSS.last_startup_time as LastStartup,
    DSS.service_account as Account
    FROM sys.dm_server_services AS DSS order by 1;

"@



#always ON Status
$haquery = @"
select c.name, b.replica_server_name,a.role_desc, a.connected_state_desc, a.synchronization_health_desc,b.endpoint_url from sys.dm_hadr_availability_replica_states as a
    inner join sys.availability_replicas  as b on a.replica_id = b.replica_id
    inner join sys.availability_groups_cluster as c on b.group_id=c.group_id
"@


# SQL Server Job informaton
$jobquery = @"
SELECT * 
FROM (
    SELECT 
        @@servername AS Server_Name,
        S.name AS JobName,
        l.name AS JobOwner,
        SS.name AS ScheduleName, 
        CASE WHEN S.enabled = 1 THEN 'Enabled' 
             ELSE 'Disabled' 
        END AS [IsEnabled],                   
        CASE (SS.freq_type)
            WHEN 1 THEN 'Once'
            WHEN 4 THEN 'Daily'
            WHEN 8 THEN 
                CASE WHEN (SS.freq_recurrence_factor > 1) 
                     THEN 'Every ' + CONVERT(VARCHAR(3), SS.freq_recurrence_factor) + ' Weeks'  
                     ELSE 'Weekly'  
                END
            WHEN 16 THEN 
                CASE WHEN (SS.freq_recurrence_factor > 1) 
                     THEN 'Every ' + CONVERT(VARCHAR(3), SS.freq_recurrence_factor) + ' Months' 
                     ELSE 'Monthly' 
                END
            WHEN 32 THEN 'Every ' + CONVERT(VARCHAR(3), SS.freq_recurrence_factor) + ' Months' -- RELATIVE
            WHEN 64 THEN 'SQL Startup'
            WHEN 128 THEN 'SQL Idle'
            ELSE '??'
        END AS Frequency,  
        CASE
            WHEN (freq_type = 1) THEN 'One time only'
            WHEN (freq_type = 4 AND freq_interval = 1) THEN 'Every Day'
            WHEN (freq_type = 4 AND freq_interval > 1) THEN 'Every ' + CONVERT(VARCHAR(10), freq_interval) + ' Days'
            WHEN (freq_type = 8) THEN (SELECT 'Weekly Schedule' = MIN(D1 + D2 + D3 + D4 + D5 + D6 + D7)
                                       FROM (
                                           SELECT SS.schedule_id,
                                                  freq_interval, 
                                                  'D1' = CASE WHEN (freq_interval & 1 <> 0) THEN 'Sun ' ELSE '' END,
                                                  'D2' = CASE WHEN (freq_interval & 2 <> 0) THEN 'Mon ' ELSE '' END,
                                                  'D3' = CASE WHEN (freq_interval & 4 <> 0) THEN 'Tue ' ELSE '' END,
                                                  'D4' = CASE WHEN (freq_interval & 8 <> 0) THEN 'Wed ' ELSE '' END,
                                                  'D5' = CASE WHEN (freq_interval & 16 <> 0) THEN 'Thu ' ELSE '' END,
                                                  'D6' = CASE WHEN (freq_interval & 32 <> 0) THEN 'Fri ' ELSE '' END,
                                                  'D7' = CASE WHEN (freq_interval & 64 <> 0) THEN 'Sat ' ELSE '' END
                                           FROM msdb..sysschedules SS
                                           WHERE freq_type = 8
                                       ) AS F
                                       WHERE schedule_id = SJ.schedule_id)
            WHEN (freq_type = 16) THEN 'Day ' + CONVERT(VARCHAR(2), freq_interval) 
            WHEN (freq_type = 32) THEN 
                (SELECT freq_rel + WDAY 
                 FROM (
                     SELECT SS.schedule_id,
                            'freq_rel' = CASE(freq_relative_interval)
                                WHEN 1 THEN 'First'
                                WHEN 2 THEN 'Second'
                                WHEN 4 THEN 'Third'
                                WHEN 8 THEN 'Fourth'
                                WHEN 16 THEN 'Last'
                                ELSE '??'
                            END,
                            'WDAY' = CASE (freq_interval)
                                WHEN 1 THEN ' Sun'
                                WHEN 2 THEN ' Mon'
                                WHEN 3 THEN ' Tue'
                                WHEN 4 THEN ' Wed'
                                WHEN 5 THEN ' Thu'
                                WHEN 6 THEN ' Fri'
                                WHEN 7 THEN ' Sat'
                                WHEN 8 THEN ' Day'
                                WHEN 9 THEN ' Weekday'
                                WHEN 10 THEN ' Weekend'
                                ELSE '??'
                            END
                     FROM msdb..sysschedules SS
                     WHERE SS.freq_type = 32
                 ) AS WS 
                 WHERE WS.schedule_id = SS.schedule_id) 
        END AS Interval,
        CASE (freq_subday_type)
            WHEN 1 THEN LEFT(STUFF(STUFF(REPLICATE('0', 6 - LEN(active_start_time)) + CONVERT(VARCHAR(6), active_start_time), 3, 0, ':'), 6, 0, ':'), 8)
            WHEN 2 THEN 'Every ' + CONVERT(VARCHAR(10), freq_subday_interval) + ' seconds'
            WHEN 4 THEN 'Every ' + CONVERT(VARCHAR(10), freq_subday_interval) + ' minutes'
            WHEN 8 THEN 'Every ' + CONVERT(VARCHAR(10), freq_subday_interval) + ' hours'
            ELSE '??'
        END AS [Time],
        CASE SJ.next_run_date
            WHEN 0 THEN CAST('n/a' AS CHAR(10))
            ELSE CONVERT(CHAR(10), CONVERT(DATETIME, CONVERT(CHAR(8), SJ.next_run_date)), 120) + ' ' + LEFT(STUFF(STUFF(REPLICATE('0', 6 - LEN(next_run_time)) + CONVERT(VARCHAR(6), next_run_time), 3, 0, ':'), 6, 0, ':'), 8)
        END AS NextRunTime,
        LastRunOutcome = CASE (SELECT TOP 1 jbh.run_status 
                               FROM msdb..sysjobhistory jbh 
                               WHERE jbh.step_id = 0 
                               AND jbh.job_id = S.job_id 
                               ORDER BY run_date DESC) 
                         WHEN 0 THEN 'Failed' 
                         WHEN 1 THEN 'Succeeded' 
                         WHEN 2 THEN 'Retry' 
                         WHEN 3 THEN 'Canceled' 
                         WHEN 4 THEN 'In Progress' 
                         ELSE '' 
                         END
    FROM msdb.dbo.sysjobs S
    LEFT JOIN msdb.dbo.sysjobschedules SJ ON S.job_id = SJ.job_id  
    LEFT JOIN msdb.dbo.sysschedules SS ON SS.schedule_id = SJ.schedule_id
    LEFT JOIN master.sys.syslogins l ON S.owner_sid = l.sid
) a
WHERE LastRunOutcome = 'Failed'
  AND IsEnabled = 'Enabled'
ORDER BY JobName;
"@


# List of Sysadmin account
$sysadminquery = @"
SELECT DISTINCT @@servername Server_Name, p.name AS [loginname] ,
p.type ,
p.type_desc ,
CONVERT(VARCHAR(10),p.create_date ,101) AS [created],
CONVERT(VARCHAR(10),p.modify_date , 101) AS [update]
FROM sys.server_principals p
JOIN sys.syslogins s ON p.sid = s.sid
JOIN sys.server_permissions sp ON p.principal_id = sp.grantee_principal_id
WHERE p.type_desc IN ('SQL_LOGIN', 'WINDOWS_LOGIN', 'WINDOWS_GROUP')
-- Logins that are not process logins
AND p.name NOT LIKE '##%' and p.name not in ('FACTENTRYUK\administrator','FACTENTRYUK\dba','sa','FACTENTRYUK\DBATeam')
-- Logins that are sysadmins or have GRANT CONTROL SERVER
AND (s.sysadmin = 1 OR sp.permission_name = 'CONTROL SERVER') and 
p.name not like 'NT%'
ORDER BY p.type,p.name 
GO

"@


# SQL Server Read_Only/Offline Database
$dbquery = @"
select @@SERVERNAME Server_Name , name, state_desc = case is_read_only  when 0 then 'OFFLINE' when 1 then 'READ_ONLY' end  
from sys.databases 
where state_desc='OFFLINE' or is_read_only=1
"@

# SQL Server MEMORY Utilisation status
$memquery = @"
DECLARE @Total AS Float
DECLARE @Target AS Float
DECLARE @Ratio AS Float
DECLARE @Min AS Float
DECLARE @Max AS Float
DECLARE @Page_File AS Float
DECLARE @Memory_Grant AS Float
DECLARE @TotalMemoryUsed AS Bigint
DECLARE @BufferPoolAllocated AS Bigint
DECLARE @Comments AS NVARCHAR(50)
DECLARE @Version AS NVARCHAR(128)

-- Set memory metrics
SET @Total = (SELECT ROUND(((CAST([cntr_value] AS FLOAT) / 1024) / 1024), 2)
              FROM sys.dm_os_performance_counters 
              WHERE [object_name] LIKE '%Memory Manager%' AND [counter_name] = 'Total Server Memory (KB)')

SET @Target = (SELECT ROUND(((CAST([cntr_value] AS FLOAT) / 1024) / 1024), 2)
               FROM sys.dm_os_performance_counters 
               WHERE [object_name] LIKE '%Memory Manager%' AND [counter_name] = 'Target Server Memory (KB)')

SET @Page_File = (SELECT [cntr_value] 
                  FROM sys.dm_os_performance_counters 
                  WHERE [object_name] LIKE '%%Buffer Manager%' AND [counter_name] LIKE '%Page life%')

SET @Memory_Grant = (SELECT [cntr_value] FROM sys.dm_os_performance_counters 
                     WHERE [object_name] LIKE '%Memory Manager%' AND [counter_name] LIKE '%Memory Grants Pending%')

SET @Ratio = (SELECT ROUND(100.0 * (SELECT CAST([cntr_value] AS FLOAT) FROM sys.dm_os_performance_counters 
                                    WHERE [object_name] LIKE '%Memory Manager%' AND [counter_name] = 'Total Server Memory (KB)') / 
                          (SELECT CAST([cntr_value] AS FLOAT) FROM sys.dm_os_performance_counters 
                           WHERE [object_name] LIKE '%Memory Manager%' AND [counter_name] = 'Target Server Memory (KB)'), 2))

SET @Min = (SELECT ROUND((CAST(value AS FLOAT) / 1024),2) FROM sys.configurations WHERE name = 'min server memory (MB)')
SET @Max = (SELECT ROUND((CAST(value AS FLOAT) / 1024),2) FROM sys.configurations WHERE name = 'max server memory (MB)')


-- Determine SQL Server version
SELECT @Version = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))

-- Set total memory used and buffer pool allocated based on version
IF (@Version LIKE '15%') -- SQL Server 2019 uses a major version of 15
BEGIN
    SELECT @TotalMemoryUsed = SUM(pages_kb + virtual_memory_committed_kb + awe_allocated_kb) / 1024
    FROM sys.dm_os_memory_clerks
END
ELSE
BEGIN
    SELECT @TotalMemoryUsed = SUM(pages_kb + virtual_memory_committed_kb + awe_allocated_kb) / 1024
    FROM sys.dm_os_memory_clerks
END

SELECT @BufferPoolAllocated = cntr_value / 1024
FROM sys.dm_os_performance_counters
WHERE counter_name = 'Target Server Memory (KB)'

-- Determine comments based on memory comparison
IF (@BufferPoolAllocated > @TotalMemoryUsed)
BEGIN
    SET @Comments = 'Memory Utilisation looks GOOD'
END
ELSE
BEGIN
    SET @Comments = 'Memory Utilistion is HIGH'
END

-- Select final results in a single table
SELECT 
    @@servername Server_Name, @Page_File AS [PageLifeExpectancy],
    @Memory_Grant AS [MemoryGrantsPending],
    @Min AS [MinServerMemory_GB],
    @Max AS [MaxServerMemory_GB],
    @Total AS [TotalServerMemory_GB],
    @Target AS [TargetServerMemory_GB],
    ROUND(@Total / @Target, 4) * 100 AS [Ratio_Total_Target],
    @TotalMemoryUsed AS [TotalMemoryUsed_MB],
    @BufferPoolAllocated AS [BufferPoolAllocated_MB],
    @Comments AS [Comments];
	
"@

#Collecting Blocking sessions 
$deadlockquery = @"
  
declare @BlockingDurationThreshold smallint = 60;  
declare @BlockedSessionThreshold smallint = NULL;
 
SET NOCOUNT ON;  
--READ UNCOMMITTED, since we're dealing with blocking, we don't want to make things worse.  
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;  
  
---Sure, it would work if you supplied both, but the ANDing of those gets confusing to people, so easier to just do this.  
IF ((@BlockingDurationThreshold IS NOT NULL AND @BlockedSessionThreshold IS NOT NULL)  
    OR COALESCE(@BlockingDurationThreshold, @BlockedSessionThreshold) IS NULL)  
BEGIN  
    RAISERROR('Must supply either @BlockingDurationThreshold or @BlockedSessionThreshold (but not both).',16,1);  
END;  
  
DECLARE @Id int = 1,  
        @Spid int = 0,  
        @JobIdHex nvarchar(34),  
        @JobName nvarchar(256),  
        @WaitResource nvarchar(256),  
        @DbName nvarchar(256),  
        @ObjectName nvarchar(256),  
        @IndexName nvarchar(256),  
        @Sql nvarchar(max);  
  
CREATE TABLE #Blocked (  
    ID int identity(1,1) PRIMARY KEY,  
    WaitingSpid smallint,  
    BlockingSpid smallint,  
    LeadingBlocker smallint,  
    BlockingChain nvarchar(4000),  
    DbName sysname,  
    HostName nvarchar(128),  
    ProgramName nvarchar(128),  
    LoginName nvarchar(128),  
    LoginTime datetime2(3),  
    LastRequestStart datetime2(3),  
    LastRequestEnd datetime2(3),  
    TransactionCnt int,  
    Command nvarchar(32),  
    WaitTime int,  
    WaitResource nvarchar(256),  
    WaitDescription nvarchar(1000),  
    SqlText nvarchar(max),  
    SqlStatement nvarchar(max),  
    InputBuffer nvarchar(4000),  
    SessionInfo XML,  
    );  
  
CREATE TABLE #InputBuffer (  
    EventType nvarchar(30),  
    Params smallint,  
    EventInfo nvarchar(4000)  
    );  
  
CREATE TABLE #LeadingBlocker (  
    Id int identity(1,1) PRIMARY KEY,  
    LeadingBlocker smallint,  
    BlockedSpidCount int,  
    DbName sysname,  
    HostName nvarchar(128),  
    ProgramName nvarchar(128),  
    LoginName nvarchar(128),  
    LoginTime datetime2(3),  
    LastRequestStart datetime2(3),  
    LastRequestEnd datetime2(3),  
    TransactionCnt int,  
    Command nvarchar(32),  
    WaitTime int,  
    WaitResource nvarchar(256),  
    WaitDescription nvarchar(1000),  
    SqlText nvarchar(max),  
    SqlStatement nvarchar(max),  
    InputBuffer nvarchar(4000),  
    SessionInfo xml,  
    );  
  
  
--Grab all sessions involved in Blocking (both blockers & waiters)  
  
INSERT INTO #Blocked (WaitingSpid, BlockingSpid, DbName, HostName, ProgramName, LoginName, LoginTime, LastRequestStart,   
                    LastRequestEnd, TransactionCnt, Command, WaitTime, WaitResource, SqlText, SqlStatement)  
-- WAITERS  
SELECT s.session_id AS WaitingSpid,   
       r.blocking_session_id AS BlockingSpid,  
       db_name(r.database_id) AS DbName,  
       s.host_name AS HostName,  
       s.program_name AS ProgramName,  
       s.login_name AS LoginName,  
       s.login_time AS LoginTime,  
       s.last_request_start_time AS LastRequestStart,  
       s.last_request_end_time AS LastRequestEnd,  
       -- Need to use sysprocesses for now until we're fully on 2012/2014  
       (SELECT TOP 1 sp.open_tran FROM master.sys.sysprocesses sp WHERE sp.spid = s.session_id) AS TransactionCnt,  
       --s.open_transaction_count AS TransactionCnt,  
       r.command AS Command,  
       r.wait_time AS WaitTime,  
       r.wait_resource AS WaitResource,  
       COALESCE(t.text,'') AS SqlText,  
       COALESCE(SUBSTRING(t.text, (r.statement_start_offset/2)+1, (  
                (CASE r.statement_end_offset  
                   WHEN -1 THEN DATALENGTH(t.text)  
                   ELSE r.statement_end_offset  
                 END - r.statement_start_offset)  
              /2) + 1),'') AS SqlStatement  
FROM sys.dm_exec_sessions s  
INNER JOIN sys.dm_exec_requests r ON r.session_id = s.session_id  
OUTER APPLY sys.dm_exec_sql_text (r.sql_handle) t  
WHERE r.blocking_session_id <> 0                --Blocked  
AND r.wait_time >= COALESCE(@BlockingDurationThreshold,0)*1000  
UNION   
-- BLOCKERS  
SELECT s.session_id AS WaitingSpid,   
       COALESCE(r.blocking_session_id,0) AS BlockingSpid,  
       COALESCE(db_name(r.database_id),'') AS DbName,  
       s.host_name AS HostName,  
       s.program_name AS ProgramName,  
       s.login_name AS LoginName,  
       s.login_time AS LoginTime,  
       s.last_request_start_time AS LastRequestStart,  
       s.last_request_end_time AS LastRequestEnd,  
       -- Need to use sysprocesses for now until we're fully on 2012/2014  
       (SELECT TOP 1 sp.open_tran FROM master.sys.sysprocesses sp WHERE sp.spid = s.session_id) AS TransactionCnt,  
       --s.open_transaction_count AS TransactionCnt,  
       COALESCE(r.command,'') AS Command,   
       COALESCE(r.wait_time,'') AS WaitTime,  
       COALESCE(r.wait_resource,'') AS WaitResource,  
       COALESCE(t.text,'') AS SqlText,  
       COALESCE(SUBSTRING(t.text, (r.statement_start_offset/2)+1, (  
                (CASE r.statement_end_offset  
                   WHEN -1 THEN DATALENGTH(t.text)  
                   ELSE r.statement_end_offset  
                 END - r.statement_start_offset)  
              /2) + 1),'') AS SqlStatement  
FROM sys.dm_exec_sessions s  
LEFT JOIN sys.dm_exec_requests r ON r.session_id = s.session_id  
OUTER APPLY sys.dm_exec_sql_text (r.sql_handle) t  
WHERE s.session_id IN (SELECT blocking_session_id FROM sys.dm_exec_requests ) --Blockers  
AND COALESCE(r.blocking_session_id,0) = 0;                  --Not blocked  
  
  
-- Grab the input buffer for all sessions, too.  
WHILE EXISTS (SELECT 1 FROM #Blocked WHERE InputBuffer IS NULL)  
BEGIN  
    TRUNCATE TABLE #InputBuffer;  
      
    SELECT TOP 1 @Spid = WaitingSpid, @ID = ID  
    FROM #Blocked  
    WHERE InputBuffer IS NULL;  
  
    SET @Sql = 'DBCC INPUTBUFFER (' + CAST(@Spid AS varchar(10)) + ');';  
  
    BEGIN TRY  
        INSERT INTO #InputBuffer  
        EXEC sp_executesql @sql;  
    END TRY  
    BEGIN CATCH  
        PRINT 'InputBuffer Failed';  
    END CATCH  
  
    --SELECT @id, @Spid, COALESCE((SELECT TOP 1 EventInfo FROM #InputBuffer),'')  
    --EXEC sp_executesql @sql;  
  
    UPDATE b  
    SET InputBuffer = COALESCE((SELECT TOP 1 EventInfo FROM #InputBuffer),'')  
    FROM #Blocked b  
    WHERE ID = @Id;  
END;  
  
--Convert Hex job_ids for SQL Agent jobs to names.  
WHILE EXISTS(SELECT 1 FROM #Blocked WHERE ProgramName LIKE 'SQLAgent - TSQL JobStep (Job 0x%')  
BEGIN  
    SELECT @JobIdHex = '', @JobName = '';  
  
    SELECT TOP 1 @ID = ID,   
            @JobIdHex =  SUBSTRING(ProgramName,30,34)  
    FROM #Blocked  
    WHERE ProgramName LIKE 'SQLAgent - TSQL JobStep (Job 0x%';  
  
    SELECT @Sql = N'SELECT @JobName = name FROM msdb.dbo.sysjobs WHERE job_id = ' + @JobIdHex;  
    EXEC sp_executesql @Sql, N'@JobName nvarchar(256) OUT', @JobName = @JobName OUT;  
  
    UPDATE b  
    SET ProgramName = LEFT(REPLACE(ProgramName,@JobIdHex,@JobName),128)  
    FROM #Blocked b  
    WHERE ID = @Id;  
END;  
  
--Decypher wait resources.  
DECLARE wait_cur CURSOR FOR  
    SELECT WaitingSpid, WaitResource FROM #Blocked WHERE WaitResource <> '';  
  
OPEN wait_cur;  
FETCH NEXT FROM wait_cur INTO @Spid, @WaitResource;  
WHILE @@FETCH_STATUS = 0  
BEGIN  
    IF @WaitResource LIKE 'KEY%'  
    BEGIN  
        --Decypher DB portion of wait resource  
        SET @WaitResource = LTRIM(REPLACE(@WaitResource,'KEY:',''));  
        SET @DbName = db_name(SUBSTRING(@WaitResource,0,CHARINDEX(':',@WaitResource)));  
        --now get the object name  
        SET @WaitResource = SUBSTRING(@WaitResource,CHARINDEX(':',@WaitResource)+1,256);  
        SELECT @Sql = 'SELECT @ObjectName = SCHEMA_NAME(o.schema_id) + ''.'' + o.name, @IndexName = i.name ' +  
            'FROM ' + QUOTENAME(@DbName) + '.sys.partitions p ' +  
            'JOIN ' + QUOTENAME(@DbName) + '.sys.objects o ON p.OBJECT_ID = o.OBJECT_ID ' +  
            'JOIN ' + QUOTENAME(@DbName) + '.sys.indexes i ON p.OBJECT_ID = i.OBJECT_ID  AND p.index_id = i.index_id ' +  
            'WHERE p.hobt_id = SUBSTRING(@WaitResource,0,CHARINDEX('' '',@WaitResource))'  
        EXEC sp_executesql @sql,N'@WaitResource nvarchar(256),@ObjectName nvarchar(256) OUT,@IndexName nvarchar(256) OUT',  
                @WaitResource = @WaitResource, @ObjectName = @ObjectName OUT, @IndexName = @IndexName OUT  
        --now populate the WaitDescription column  
        UPDATE b  
        SET WaitDescription = 'KEY WAIT: ' + @DbName + '.' + @ObjectName + ' (' + COALESCE(@IndexName,'') + ')'  
        FROM #Blocked b  
        WHERE WaitingSpid = @Spid;  
    END;  
    ELSE IF @WaitResource LIKE 'OBJECT%'  
    BEGIN  
        --Decypher DB portion of wait resource  
        SET @WaitResource = LTRIM(REPLACE(@WaitResource,'OBJECT:',''));  
        SET @DbName = db_name(SUBSTRING(@WaitResource,0,CHARINDEX(':',@WaitResource)));  
        --now get the object name  
        SET @WaitResource = SUBSTRING(@WaitResource,CHARINDEX(':',@WaitResource)+1,256);  
        SET @Sql = 'SELECT @ObjectName = schema_name(schema_id) + ''.'' + name FROM [' + @DbName + '].sys.objects WHERE object_id = SUBSTRING(@WaitResource,0,CHARINDEX('':'',@WaitResource))';  
        EXEC sp_executesql @sql,N'@WaitResource nvarchar(256),@ObjectName nvarchar(256) OUT',@WaitResource = @WaitResource, @ObjectName = @ObjectName OUT;  
        --Now populate the WaitDescription column  
        UPDATE b  
        SET WaitDescription = 'OBJECT WAIT: ' + @DbName + '.' + @ObjectName  
        FROM #Blocked b  
        WHERE WaitingSpid = @Spid;  
    END;  
    ELSE IF (@WaitResource LIKE 'PAGE%' OR @WaitResource LIKE 'RID%')  
    BEGIN  
        --Decypher DB portion of wait resource  
        SELECT @WaitResource = LTRIM(REPLACE(@WaitResource,'PAGE:',''));  
        SELECT @WaitResource = LTRIM(REPLACE(@WaitResource,'RID:',''));  
        SET @DbName = db_name(SUBSTRING(@WaitResource,0,CHARINDEX(':',@WaitResource)));  
        --now get the file name  
        SET @WaitResource = SUBSTRING(@WaitResource,CHARINDEX(':',@WaitResource)+1,256)  
        SELECT @ObjectName = name   
        FROM sys.master_files  
        WHERE database_id = db_id(@DbName)  
        AND file_id = SUBSTRING(@WaitResource,0,CHARINDEX(':',@WaitResource));  
        --Now populate the WaitDescription column  
        SET @WaitResource = SUBSTRING(@WaitResource,CHARINDEX(':',@WaitResource)+1,256)  
        IF @WaitResource LIKE '%:%'  
        BEGIN  
            UPDATE b  
            SET WaitDescription = 'ROW WAIT: ' + @DbName + ' File: ' + @ObjectName + ' Page_id/Slot: ' + @WaitResource  
            FROM #Blocked b  
            WHERE WaitingSpid = @Spid;  
        END;  
        ELSE  
        BEGIN  
            UPDATE b  
            SET WaitDescription = 'PAGE WAIT: ' + @DbName + ' File: ' + @ObjectName + ' Page_id: ' + @WaitResource  
            FROM #Blocked b  
            WHERE WaitingSpid = @Spid;  
        END;  
    END;  
    FETCH NEXT FROM wait_cur INTO @Spid, @WaitResource;  
END;  
CLOSE wait_cur;  
DEALLOCATE wait_cur;  
  
  
--Move the LEADING blockers out to their own table.  
INSERT INTO #LeadingBlocker (LeadingBlocker, DbName, HostName, ProgramName, LoginName, LoginTime, LastRequestStart, LastRequestEnd,   
                    TransactionCnt, Command, WaitTime, WaitResource, WaitDescription, SqlText, SqlStatement, InputBuffer)  
SELECT WaitingSpid, DbName, HostName, ProgramName, LoginName, LoginTime, LastRequestStart, LastRequestEnd,   
                    TransactionCnt, Command, WaitTime, WaitResource, WaitDescription, SqlText, SqlStatement, InputBuffer  
FROM #Blocked b  
WHERE BlockingSpid = 0  
AND EXISTS (SELECT 1 FROM #Blocked b1 WHERE b1.BlockingSpid = b.WaitingSpid);  
  
DELETE FROM #Blocked WHERE BlockingSpid = 0;  
  
--Update #Blocked to include LeadingBlocker & BlockingChain  
WITH BlockingChain AS (  
    SELECT LeadingBlocker AS Spid,   
           CAST(0 AS smallint) AS Blocker,  
           CAST(LeadingBlocker AS nvarchar(4000)) AS BlockingChain,   
           LeadingBlocker AS LeadingBlocker  
    FROM #LeadingBlocker  
    UNION ALL  
    SELECT b.WaitingSpid AS Spid,   
           b.BlockingSpid AS Blocker,  
           RIGHT((CAST(b.WaitingSpid AS nvarchar(10)) + N' ' + CHAR(187) + N' ' + bc.BlockingChain),4000) AS BlockingChain,  
           bc.LeadingBlocker  
    FROM #Blocked b  
    JOIN BlockingChain bc ON bc.Spid = b.BlockingSpid  
    )  
UPDATE b  
SET LeadingBlocker = bc.LeadingBlocker,  
    BlockingChain = bc.BlockingChain  
FROM #Blocked b  
JOIN BlockingChain bc ON b.WaitingSpid = bc.Spid;  
  
-- Populate BlockedSpidCount for #LeadingBlocker  
UPDATE lb  
SET BlockedSpidCount = cnt.BlockedSpidCount  
FROM #LeadingBlocker lb  
JOIN (SELECT LeadingBlocker, COUNT(*) BlockedSpidCount FROM #Blocked GROUP BY LeadingBlocker) cnt   
        ON cnt.LeadingBlocker = lb.LeadingBlocker;  
  
  
-- Populate SessionInfo column with HTML details for sending email  
-- Since there's a bunch of logic here, code is more readable doing this separate than mashing it in with the rest of HTML email creation  
  
UPDATE lb  
SET SessionInfo = (SELECT LeadingBlocker,  
                          LoginName,   
                          TransactionCnt,   
                          WaitResource = COALESCE(WaitDescription,WaitResource),  
                          HostName,  
                          DbName,  
                          LastRequest = CONVERT(varchar(20),LastRequestStart,20),  
                          ProgramName,  
                          InputBuffer,  
                          SqlStatement,  
                          SqlText  
                    FROM #LeadingBlocker lb2   
                    WHERE lb.id = lb2.id   
                    FOR XML PATH ('LeadBlocker'))  
FROM #LeadingBlocker lb;  
  
  
/*UPDATE b  
SET SessionInfo = '<LoginName>' + LoginName + '</LoginName>' +  
                  '<HostName>' + HostName + '</HostName>' +  
                  CASE WHEN TransactionCnt <> 0   
                    THEN '<TransactionCnt>' + CAST(TransactionCnt AS nvarchar(10)) + '</TransactionCnt>'   
                    ELSE ''  
                  END +  
                  CASE WHEN WaitResource <> ''  
                    THEN '<WaitResource>' + COALESCE(WaitDescription,WaitResource) + '</WaitResource>'   
                    ELSE ''  
                  END +  
                  '<DbName>' + DbName + '</DbName>' +  
                  '<LastRequest>' + CONVERT(varchar(20),LastRequestStart,20) + '</LastRequest>' +  
                  '<ProgramName>' + ProgramName + '</ProgramName>'  
FROM #Blocked b;  
*/  
UPDATE b  
SET SessionInfo = (SELECT WaitingSpid,  
                          BlockingChain,  
                          LoginName,   
                          TransactionCnt,   
                          WaitResource = COALESCE(WaitDescription,WaitResource),  
                          HostName,  
                          DbName,  
                          LastRequest = CONVERT(varchar(20),LastRequestStart,20),  
                          ProgramName,  
                          InputBuffer,  
                          SqlStatement,  
                          SqlText  
                    FROM #Blocked b2   
                    WHERE b.id = b2.id   
                    FOR XML PATH ('BlockedSession'))  
FROM #Blocked b;  
  
--output results  
    IF NOT EXISTS (SELECT 1 FROM #LeadingBlocker WHERE BlockedSpidCount >= COALESCE(@BlockedSessionThreshold,BlockedSpidCount))  
        SELECT @@SERVERNAME servername, 'No Blocking Detected' AS Blocking;  
    ELSE  
    BEGIN  
        --SELECT * FROM #LeadingBlocker   
        --WHERE BlockedSpidCount >= COALESCE(@BlockedSessionThreshold,BlockedSpidCount)  
        --ORDER BY LoginTime;  
        ----  
        --SELECT * FROM #Blocked b  
        --WHERE EXISTS (SELECT 1 FROM #LeadingBlocker lb   
        --                WHERE lb.LeadingBlocker = b.LeadingBlocker  
        --                AND lb.BlockedSpidCount >= COALESCE(@BlockedSessionThreshold,lb.BlockedSpidCount))  
        --ORDER BY b.WaitTime DESC;  
		select @@SERVERNAME servername,'Blocking Detected' AS Blocking,a.HostName,b.DbName,a.LoginName,a.LeadingBlocker,b.WaitingSpid,b.Command ,a.LoginTime,a.LastRequestStart,a.LastRequestEnd,b.BlockingChain,b.WaitResource,a.SqlText [leading_statement],b.InputBuffer[waiting_query],a.SessionInfo
		FROM #LeadingBlocker a, #Blocked b
    END; 
	 
	drop table #Blocked
	drop table #InputBuffer
	drop table #LeadingBlocker
"@

$tempquery = @"
	--Tempdb file usage information
set nocount on
Create table #tempdbfileusage(               
servername varchar(100),                           
databasename varchar(100),                           
filename varchar(100),                           
physicalName varchar(100),                           
filesizeMB int,                           
availableSpaceMB int,                           
percentfull int   
)   
  
DECLARE @TEMPDBSQL NVARCHAR(4000);  
SET @TEMPDBSQL = ' USE Tempdb;  
SELECT  CONVERT(VARCHAR(100), @@SERVERNAME) AS [server_name]  
                ,db.name AS [database_name]  
                ,mf.[name] AS [file_logical_name]  
                ,mf.[filename] AS[file_physical_name]  
                ,convert(FLOAT, mf.[size]/128) AS [file_size_mb]               
                ,convert(FLOAT, (mf.[size]/128 - (CAST(FILEPROPERTY(mf.[name], ''SpaceUsed'') AS int)/128))) as [available_space_mb]  
                ,convert(DECIMAL(38,2), (CAST(FILEPROPERTY(mf.[name], ''SpaceUsed'') AS int)/128.0)/(mf.[size]/128.0))*100 as [percent_full]      
FROM   tempdb.dbo.sysfiles mf  
JOIN      master..sysdatabases db  
ON         db.dbid = db_id()';  
--PRINT @TEMPDBSQL;  
insert into #tempdbfileusage  
EXEC SP_EXECUTESQL @TEMPDBSQL; 
if (select max(percentfull) from #tempdbfileusage) >=50.00
select 'Tempdb Running OUT OF Threshold' [status], * from #tempdbfileusage
else 
select @@SERVERNAME servername,'Tempdb' databasename,avg(filesizeMB) filesizeMB,avg(availableSpaceMB) availableSpaceMB,avg(percentfull) percentfull,'Tempdb Running Within the Threshold' [status] from #tempdbfileusage 
drop table #tempdbfileusage
	
"@

#Error Log collection
$errorlogquery = @"
declare @errorlogcount int
        IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects WHERE ID = OBJECT_ID(N'tempdb..#errorlog'))
        BEGIN
        DROP TABLE #errorlog
        END
        create table #errorlog(date_time datetime,processinfo varchar(123),Comments varchar(max))
        insert into #errorlog exec sp_readerrorlog
		--select * from #errorlog
        select @errorlogcount = count(*) from #errorlog 
        where date_time > (CONVERT(datetime,getdate()) - 0.2)
        and Comments like '%fail%' 
        and Comments like '%error%'
        and processinfo not in ('Server','Logon')

        if(@errorlogcount >= 1)
        begin
        select @@servername servername,date_time as Date,processinfo as ProcessInfo, Comments from #errorlog 
        where date_time > (CONVERT(datetime,getdate()) - 0.2)
        and Comments like '%fail%' 
        and Comments like '%error%'
        and processinfo not in ('Server','Logon')
        end
        else
        begin
        select top 1 @@servername servername,date_time date, 'Check did not find out anything major' as ProcessInfo, 'But will still advise to please verify manually' as Comments from #errorlog
        end

"@

#Linkedserver

$linkedserverquery = @" 
SELECT @@servername servername,a.server_id,a.name,a.product,a.provider,a.data_source,a.location,a.provider_string
        FROM sys.Servers a
        LEFT OUTER JOIN sys.linked_logins b ON b.server_id = a.server_id
        LEFT OUTER JOIN sys.server_principals c ON c.principal_id = b.local_principal_id
"@

$URquery = @"
SELECT DISTINCT
    SERVERPROPERTY('servername') AS ServerName
"@

$instanceleveldbgrowthquery = @"
DECLARE @endDate DATETIME, @startDate DATETIME, @currentMonth INT;
SET @endDate = GETDATE();  -- Current date
SET @startDate = DATEFROMPARTS(YEAR(@endDate), 1, 1);  -- Start of the current year
SET @currentMonth = DATEPART(MM, @endDate);  -- Get the current month

;WITH HIST AS
(
    SELECT 
        MONTH(BS.backup_start_date) AS Month,
        CONVERT(NUMERIC(10, 2), AVG(BF.file_size / 1048576.0)) AS AvgSizeMB
    FROM msdb.dbo.backupset AS BS
    INNER JOIN msdb.dbo.backupfile AS BF
        ON BS.backup_set_id = BF.backup_set_id
    WHERE 
        BS.database_name NOT IN ('master', 'msdb', 'model', 'tempdb')
        AND BF.file_type = 'D'
        AND BS.backup_start_date BETWEEN @startDate AND @endDate
    GROUP BY 
        MONTH(BS.backup_start_date)
),
PIVOTED AS
(
    SELECT 
        ISNULL([1], 0) AS January,
        ISNULL([2], 0) AS February,
        ISNULL([3], 0) AS March,
        ISNULL([4], 0) AS April,
        ISNULL([5], 0) AS May,
        ISNULL([6], 0) AS June,
        ISNULL([7], 0) AS July,
        ISNULL([8], 0) AS August,
        ISNULL([9], 0) AS September,
        ISNULL([10], 0) AS October,
        ISNULL([11], 0) AS November,
        ISNULL([12], 0) AS December
    FROM
        (SELECT 
            Month,
            AvgSizeMB
        FROM HIST
        ) AS SourceTable
    PIVOT
    (
        SUM(AvgSizeMB)
        FOR Month IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12])
    ) AS PivotTable
),
EarliestMonth AS
(
    SELECT
        CASE 
            WHEN January > 0 THEN 1
            WHEN February > 0 THEN 2
            WHEN March > 0 THEN 3
            WHEN April > 0 THEN 4
            WHEN May > 0 THEN 5
            WHEN June > 0 THEN 6
            WHEN July > 0 THEN 7
            WHEN August > 0 THEN 8
            WHEN September > 0 THEN 9
            WHEN October > 0 THEN 10
            WHEN November > 0 THEN 11
            WHEN December > 0 THEN 12
            ELSE NULL
        END AS EarliestAvailableMonth,
        CASE 
            WHEN January > 0 THEN January
            WHEN February > 0 THEN February
            WHEN March > 0 THEN March
            WHEN April > 0 THEN April
            WHEN May > 0 THEN May
            WHEN June > 0 THEN June
            WHEN July > 0 THEN July
            WHEN August > 0 THEN August
            WHEN September > 0 THEN September
            WHEN October > 0 THEN October
            WHEN November > 0 THEN November
            WHEN December > 0 THEN December
            ELSE NULL
        END AS EarliestMonthSize
    FROM 
        PIVOTED
),
CurrentMonthData AS
(
    SELECT 
        CASE @currentMonth 
            WHEN 1 THEN January
            WHEN 2 THEN February
            WHEN 3 THEN March
            WHEN 4 THEN April
            WHEN 5 THEN May
            WHEN 6 THEN June
            WHEN 7 THEN July
            WHEN 8 THEN August
            WHEN 9 THEN September
            WHEN 10 THEN October
            WHEN 11 THEN November
            WHEN 12 THEN December
        END AS CurrentMonthSize
    FROM 
        PIVOTED
),
Growth AS
(
    SELECT 
        CASE 
            WHEN EarliestMonthSize > 0 THEN FORMAT(
                ((CurrentMonthSize - EarliestMonthSize) / NULLIF(EarliestMonthSize, 0)) * 100, '0.00')
            ELSE 'N/A'
        END AS GrowthPercentage
    FROM 
        CurrentMonthData
    CROSS JOIN
        EarliestMonth
)
SELECT 
    @@SERVERNAME AS ServerName,
    January,
    February,
    March,
    April,
    May,
    June,
    July,
    August,
    September,
    October,
    November,
    December,
    GrowthPercentage
FROM 
    PIVOTED
CROSS JOIN 
    Growth
ORDER BY 
    ServerName;


"@

$NewlyCretedJobsq = @"
SELECT  j.name AS job_name, j.date_created,
    CASE 
        WHEN j.enabled = 1 THEN 'Enabled'
        ELSE 'Disabled'
    END AS job_status,
    CASE 
        WHEN h.run_status = 1 THEN 'Success'
        WHEN h.run_status = 0 THEN 'Failure'
        WHEN h.run_status = 4 THEN 'Running'
        ELSE 'Other'
    END AS last_run_status,
    h.run_date AS last_run_date,
    CAST(
        ROUND(
            (ISNULL(h.run_duration / 10000, 0) * 60 +               -- Hours to minutes
             ISNULL((h.run_duration % 10000) / 100, 0) +            -- Minutes
             ISNULL(h.run_duration % 100, 0) / 60.0                 -- Seconds to minutes
            ), 2
        ) AS DECIMAL(10, 2)
    ) AS last_run_duration_in_minutes,
    s.name AS schedule_name,
    CASE 
        WHEN s.enabled = 1 THEN 'Yes'
        ELSE 'No'
    END AS schedule_enabled
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
    AND h.instance_id = ( SELECT MAX(instance_id) FROM msdb.dbo.sysjobhistory WHERE job_id = j.job_id )
LEFT JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
LEFT JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
WHERE CONVERT(date, j.date_created) = CONVERT(date, DATEADD(day, -1, GETDATE()))
ORDER BY j.date_created;

"@


$tableover10GBquery = @"
-- Create a temporary table to store results from all databases
CREATE TABLE #TableSizeInfo (
    Server_Name NVARCHAR(128),
    Database_Name NVARCHAR(128),
    Schema_Name NVARCHAR(128),
    Table_Name NVARCHAR(128),
    GB DECIMAL(18, 2),
    Compression NVARCHAR(128)
);

DECLARE @SQL NVARCHAR(MAX) = N'';

-- Generate the SQL to get the table info from all databases
SELECT @SQL = @SQL + '
USE [' + name + '];
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

INSERT INTO #TableSizeInfo (Server_Name, Database_Name, Schema_Name, Table_Name, GB, Compression)
SELECT 
    @@SERVERNAME AS Server_Name,
    ''' + name + ''' AS Database_Name,
    OBJECT_SCHEMA_NAME(t.object_id) AS Schema_Name,
    t.name AS Table_Name,
    (
        SELECT SUM(a.total_pages) / 128 / 1024
        FROM sys.partitions AS p
        INNER JOIN sys.allocation_units AS a ON a.container_id = p.partition_id
        WHERE p.object_id = t.object_id
    ) AS GB,
    (
        SELECT CASE 
                    WHEN MAX([data_compression]) = 0 
                        THEN ''Uncompressed''
                    WHEN MIN([data_compression]) = 0 AND MAX([data_compression]) > 0
                        THEN ''Partially Compressed''
                    WHEN MIN([data_compression]) = 1 AND MAX([data_compression]) = 1
                        THEN ''Row Compressed''
                    WHEN MIN([data_compression]) = 2 AND MAX([data_compression]) = 2
                        THEN ''Page Compressed''
                    WHEN MIN([data_compression]) = 1 AND MAX([data_compression]) = 2
                        THEN ''Row & Page Compressed''
                    WHEN MIN([data_compression]) IN (3,4)
                        THEN ''ColumnStore Compression''
                END
        FROM sys.partitions p
        INNER JOIN sys.allocation_units AS a ON a.container_id = p.partition_id
        WHERE p.object_id = t.object_id
    ) AS Compression
FROM sys.tables t
GROUP BY 
    t.name,
    t.object_id
HAVING (
    SELECT SUM(a.total_pages) / 128 / 1024
    FROM sys.partitions AS p
    INNER JOIN sys.allocation_units AS a ON a.container_id = p.partition_id
    WHERE p.object_id = t.object_id
) > 10
ORDER BY GB DESC;
' 
FROM sys.databases
WHERE state_desc = 'ONLINE' AND name NOT IN ('master', 'tempdb', 'model', 'msdb'); -- Exclude system databases if necessary

-- Execute the generated SQL
EXEC sp_executesql @SQL;

-- Select the results from the temporary table
SELECT * FROM #TableSizeInfo
ORDER BY GB DESC;

-- Drop the temporary table
DROP TABLE #TableSizeInfo;

"@




# Collect data
$data = @()
$service = @()
$hainfo = @()
$job = @()
$backupData = @()
$cpudata = @()
$diskdata = @()
$db = @()
$sysadmin = @()
$memdata = @()
$deadlockdata = @()
$tempdata = @()
$errorlogdata = @()
$linkedserverdata = @()
$serverStatus = @()
$URSdata  = @()
$tableover10data  = @()
$offlineServers = @()
$dbgrowthdata = @()
$NewlyCretedJobs = @()

foreach ($server in $servers) 
{
    $connectionString = "Server=$server;Database=master;Integrated Security=True;"
    
    $tableover10GBquery
    # Collect tableover10DBdata
    try {
    $10GBresult = Invoke-Sqlcmd -Query $tableover10GBquery -ConnectionString $connectionString -ErrorAction Stop
    $tableover10data += $10GBresult
    } catch {
            }

    #NewlyCretedJobs
    
    try {
    $NewlyCretedJobsresult = Invoke-Sqlcmd -Query $NewlyCretedJobsq -ConnectionString $connectionString -ErrorAction Stop
    $NewlyCretedJobs += $NewlyCretedJobsresult
    } catch {
            }

    #Instancelevel DB growth
    
    try {
    $dbgrowthresult = Invoke-Sqlcmd -Query $instanceleveldbgrowthquery -ConnectionString $connectionString -ErrorAction Stop
    $dbgrowthdata += $dbgrowthresult
    } catch {
            }

    # Collect server Deadlock metrics
    try {
    $deadlockresult = Invoke-Sqlcmd -Query $deadlockquery -ConnectionString $connectionString -ErrorAction Stop
    $deadlockdata += $deadlockresult
    } catch {
            }

    # Collect Linkedserver metrics
    try {
    $linkedserveresult = Invoke-Sqlcmd -Query $linkedserverquery -ConnectionString $connectionString -ErrorAction Stop
    $linkedserverdata += $linkedserveresult
    } catch {
            }

    # Collect server Errorlog metrics
    try {
    $errorlogresult = Invoke-Sqlcmd -Query $errorlogquery -ConnectionString $connectionString -ErrorAction Stop
    $errorlogdata += $errorlogresult
    } catch {
            }

    # Collect server Memory metrics
    try {
    $memresult = Invoke-Sqlcmd -Query $memquery -ConnectionString $connectionString -ErrorAction Stop
    $memdata += $memresult
    } catch {
            }

    # Collect server metrics
    #try {
    #$result = Invoke-Sqlcmd -Query $query -ConnectionString $connectionString -ErrorAction Stop
    #$data += $result
    #} catch {
    #        }

    # Collect sysadmin accounts
    try {
    $sysadminresult = Invoke-Sqlcmd -Query $sysadminquery -ConnectionString $connectionString -ErrorAction Stop
    $sysadmin += $sysadminresult
    } catch {
            }

     # Collect server metrics
     try {
    $dbresult = Invoke-Sqlcmd -Query $dbquery -ConnectionString $connectionString -ErrorAction Stop
    $db += $dbresult
    } catch {
            }

    # Collect service metrics
    try {
    $serviceresult = Invoke-Sqlcmd -Query $servicequery -ConnectionString $connectionString -ErrorAction Stop
    $service += $serviceresult
    } catch {
            }

    # Collect Job metrics
    try {
    $jobresult = Invoke-Sqlcmd -Query $jobquery -ConnectionString $connectionString -ErrorAction Stop
    $job += $jobresult
    } catch {
            }

    # HA information
    try {
    $haresult = Invoke-Sqlcmd -Query $haquery -ConnectionString $connectionString -ErrorAction Stop
    $hainfo += $haresult
    } catch {
            }       
    
    # Collect backup report
    try {
    $backupResult = Invoke-Sqlcmd -Query $backupQuery -ConnectionString $connectionString -ErrorAction Stop
    $backupData += $backupResult
    } catch {
            }

    # Collect CPU report
    try {
    $cpuResult = Invoke-Sqlcmd -Query $cpu -ConnectionString $connectionString -ErrorAction Stop
    $cpudata += $cpuResult
    } catch {
            }

    # Collect DISK report
    try {
    $diskResult = Invoke-Sqlcmd -Query $diskquery -ConnectionString $connectionString -ErrorAction Stop
    $diskdata += $diskResult
    } catch {
            }

    # Collect server metrics
    try {
    $tempresult = Invoke-Sqlcmd -Query $tempquery -ConnectionString $connectionString -ErrorAction Stop
    $tempdata += $tempresult
    } catch {
            }
}

# Check each server's availability
# Collect data for each server
foreach ($server in $servers) 
{
    $connectionString = "Server=$server;Database=master;Integrated Security=True;"
    
    try {
        $result = Invoke-Sqlcmd -Query $query -ConnectionString $connectionString -ErrorAction Stop
        $data += $result
    } catch {
        # If server is unreachable, add to offline servers list
        $offlineServers += [PSCustomObject]@{
            ServerName = $server
            Status = "Offline"
        }
    }
}


# Convert data to HTML
$html = @"
<!DOCTYPE html>
<html>
<head>
    <title>SQL Server Health Check Report</title>
    <meta http-equiv="refresh" content="160">
    <style>
        body { font-family: Arial, sans-serif; }
        table { width: 100%; border-collapse: collapse; }
        th, td { border: 1px solid #ddd; padding: 8px; }
        th { background-color: #AFB7BF; }
        .critical { background-color: #ffcccc; }
        .warning { background-color: #ffffcc; }
        .good { background-color: #ccffcc; }
        td.status-offline { background-color: red; } /*  Red */
    </style>
</head>
<body>
    <h1 style="color: #394655;"> SQL Server Heatlh Check Report</h1>

    <p>Report generated on: $(Get-Date)</p>
    <h2 style="color: #DF9229;">Server Metrics</h2>
    <table>
        <tr>
            <th>Server</th>
            <th>Environment</th>
            <th>Status</th>
            <th>SQL_Version</th>
            <th>SQL Version</th>
            <th>Product Level</th>
            <th>Edition</th>
            <th>Online Databases</th>
            <th>Active Requests</th>            
            <th>No_Of_Processors</th>
            <th>Tot_DB_Size_GB</th>
            <th>TotPhysicalMemory</th>
            <th>SQL Memory(MB)</th>
        </tr>
"@

foreach ($row in $data) {
switch ($row.Status) {
        "Critical" { $statusColor = "red" }
        "Offline" { $statusColor = "red" }
        "Stopped" { $statusColor = "red" }
        "Warning" { $statusColor = "yellow" }
        "Good" { $statusColor = "green" }
        "Running" { $statusColor = "lightgreen" }
        "Online" { $statusColor = "lightgreen" }
    }
    $html += "<tr>"
    $html += "<td>$($row.ServerName)</td>"
    $html += "<td>$($row.Env)</td>"
    $html += "<td style='background-color: $statusColor;'>$($row.Status)</td>"
    $html += "<td>$($row.SQL_Version)</td>"
    $html += "<td>$($row.SQLVersion)</td>"
    $html += "<td>$($row.ProductLevel)</td>"
    $html += "<td>$($row.Edition)</td>"
    $html += "<td style='text-align: center;'>$($row.OnlineDatabases)</td>"    
    $html += "<td style='text-align: center;'>$($row.ActiveRequests)</td>"    
    $html += "<td style='text-align: center;'>$($row.No_Of_Processors)</td>"
    $html += "<td style='text-align: right;'>$($row.Tot_DB_Size_GB)</td>"
    $html += "<td style='text-align: right;'>$($row.TotPhysicalMemory)</td>"
    $html += "<td style='text-align: right;'>$($row.SQL_Memory)</td>"
    $html += "</tr>"
}


#Instance level db growth
$html += @"
    </table>
    <h2 style="color: #DF9229;">Instance Level Databse Growth</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>January</th>
            <th>February</th>
            <th>March</th>
            <th>April</th>
            <th>May</th>
            <th>June</th>
            <th>July</th>
            <th>August</th>
            <th>September</th>
            <th>October</th>
            <th>November</th>
            <th>December</th>
            <th>GrowthPercentage</th>                     
        </tr>
"@
					
foreach ($row in $dbgrowthdata) {

    $html += "<tr>"
    $html += "<td>$($row.ServerName)</td>"
    $html += "<td style='text-align: right;'>$($row.January)</td>"
    $html += "<td style='text-align: right;'>$($row.February)</td>"
    $html += "<td style='text-align: right;'>$($row.March)</td>"
    $html += "<td style='text-align: right;'>$($row.April)</td>"
    $html += "<td style='text-align: right;'>$($row.May)</td>"
    $html += "<td style='text-align: right;'>$($row.June)</td>"
    $html += "<td style='text-align: right;'>$($row.July)</td>"
    $html += "<td style='text-align: right;'>$($row.August)</td>"
    $html += "<td style='text-align: right;'>$($row.September)</td>"
    $html += "<td style='text-align: right;'>$($row.October)</td>"
    $html += "<td style='text-align: right;'>$($row.November)</td>"
    $html += "<td style='text-align: right;'>$($row.December)</td>"
    $html += "<td style='text-align: right;'>$($row.GrowthPercentage)</td>" 
    $html += "</tr>"
}
#ServerName	January	February	March	April	May	June	July	August	September	October	November	December	GrowthPercentage

#Service status table
$html += @"
    </table>
    <h2 style="color: #DF9229;">SQL Server Service Status</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>Service Name</th>
            <th>StartUp Type</th>
            <th>Status</th>
            <th>Last Startup</th>
            <th>Account</th>         
        </tr>
"@
					
foreach ($row in $service) {
$statusColor = ""
    switch ($row.Status) {
        "Critical" { $statusColor = "red" }
        "Offline" { $statusColor = "red" }
        "Stopped" { $statusColor = "red" }
        "Warning" { $statusColor = "yellow" }
        "Good" { $statusColor = "green" }
        "Running" { $statusColor = "lightgreen" }
        "Online" { $statusColor = "lightgreen" }
    }
    $html += "<tr>"
    $html += "<td>$($row.Server_Name)</td>"
    $html += "<td>$($row.ServiceName)</td>"
    $html += "<td style='text-align: center;'>$($row.StartUpType)</td>"
    $html += "<td style='background-color: $statusColor;text-align: center;'>$($row.Status)</td>"
    #$html += "<td>$($row.Status)</td>"
    $html += "<td>$($row.LastStartup)</td>"
    $html += "<td>$($row.Account)</td>"       
    $html += "</tr>"
}

#Read-Only/Offline Database list
$html += @"
    </table>
    <h2 style="color: #DF9229;">Read-Only/Offline Database list</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>Database Name</th>
            <th>Status</th>                      					
        </tr>
"@
		
foreach ($row in $db) {
    switch ($row.state_desc) {
        "Critical" { $statusColor = "red" }
        "Offline"  { $statusColor = "red" }
        "READ_ONLY" { $statusColor = "yellow" }
        "OFFLINE"  { $statusColor = "red" }
        "Stopped"  { $statusColor = "red" }
        "Warning"  { $statusColor = "yellow" }
        "Good"     { $statusColor = "green" }
        "Running"  { $statusColor = "lightgreen" }
        "Online"   { $statusColor = "lightgreen" }
        
                            }
    $html += "<tr>"
    $html += "<td>$($row.Server_Name)</td>"
    $html += "<td>$($row.name)</td>"
    $html += "<td style='background-color: $statusColor;'>$($row.state_desc)</td>"
    #$html += "<td>$($row.Status)</td>"    
    $html += "</tr>"
}

#Always ON  status table
$html += @"
    </table>
    <h2 style="color: #DF9229;">SQL Server Always ON Status</h2>
    <table>
        <tr>
            
            <th>AG Name</th>
            <th>Replica Name</th>
            <th>Role</th>
            <th>Connection State</th>
            <th>Synchronization</th>
            <th>Endpoint url</th>         
        </tr>
"@
					
foreach ($row in $hainfo) {
switch ($row.synchronization_health_desc) {
        "Critical" { $statusColor = "red" }
        "PARTIALLY_HEALTHY" { $statusColor = "red" }
        "NOT_HEALTHY" { $statusColor = "red" }
        "Offline" { $statusColor = "red" }
        "READ_ONLY" { $statusColor = "yellow" }
        "OFFLINE" { $statusColor = "red" }
        "Stopped" { $statusColor = "red" }
        "Warning" { $statusColor = "yellow" }
        "Good" { $statusColor = "green" }
        "Running" { $statusColor = "lightgreen" }
        "HEALTHY" { $statusColor = "lightgreen" }
        " " { $statusColor = "white" }
        
    }
    $html += "<tr>"
    #$html += "<td>$($row.HA)</td>"
    $html += "<td>$($row.name)</td>"
    $html += "<td>$($row.replica_server_name)</td>"
    $html += "<td style='text-align: center;'>$($row.role_desc)</td>"
    $html += "<td style='text-align: center;'>$($row.connected_state_desc)</td>"
    $html += "<td style='background-color: $statusColor; text-align: center;'>$($row.synchronization_health_desc)</td>"
    #$html += "<td style='background-color: $($row.statusColor); text-align: center;'>$($row.synchronization_health_desc)</td>"
    #$html += "<td>$($row.synchronization_health_desc)</td>"
    $html += "<td>$($row.endpoint_url)</td>"       
    $html += "</tr>"
}


#Tables Over 10GB report
$html += @"
    </table>
    <h2 style="color: #DF9229;">Tables Over 10GB report</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>Database_Name</th>
            <th>Schema_Name</th>
            <th>Table_Name</th>
            <th>GB</th>
            
            </tr>
"@

foreach ($row in $tableover10data) {
      
    $html += "<tr>"
    $html += "<td>$($row.Server_Name)</td>"
    $html += "<td>$($row.Database_Name)</td>"
    $html += "<td>$($row.Schema_Name)</td>"
    $html += "<td style='text-align: right;'>$($row.Table_Name)</td>"
    $html += "<td style='text-align: right;'>$($row.GB)</td>"
    #$html += "<td style='text-align: right;'>$($row.Compression)</td>"    
    $html += "</tr>"
}

#Server_Name	Database_Name	Schema_Name	Table_Name	GB	Compression

#CPU table
$html += @"
    </table>
    <h2 style="color: #DF9229;">CPU Utilization Report</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>Number of physical CPUs</th>
            <th>Number of cores per CPU</th>
            <th>Total number of cores</th>
            <th>Number of virtual cpus</th>
            <th>CPU category</th>
            <th>AVG Utilization</th>
            <th>Status</th>
            </tr>
"@

foreach ($row in $cpudata) {
switch ($row.synchronization_health_desc) {
        "Memory Utilistion is HIGH" { $statusColor = "red" }
        "Warning" { $statusColor = "yellow" }
        "Good" { $statusColor = "green" }
        "Memory Utilisation looks GOOD" { $statusColor = "lightgreen" }
        "HEALTHY" { $statusColor = "lightgreen" }
        " " { $statusColor = "white" }
        
    }
    $html += "<tr>"
    $html += "<td>$($row.servername)</td>"
    $html += "<td>$($row.number_of_physical_cpus)</td>"
    $html += "<td>$($row.number_of_cores_per_cpu)</td>"
    $html += "<td style='text-align: right;'>$($row.total_number_of_cores)</td>"
    $html += "<td style='text-align: right;'>$($row.number_of_virtual_cpus)</td>"
    $html += "<td style='text-align: right;'>$($row.cpu_category)</td>"
    $html += "<td style='text-align: right;'>$($row.AVG_Utilization)</td>"
    $html += "<td style='background-color: $statusColor; text-align: left;'>$($row.Status)</td>"       
    $html += "</tr>"
}

#servername	number_of_physical_cpus	number_of_cores_per_cpu	total_number_of_cores	number_of_virtual_cpus	cpu_category	AVG_Utilization	Status

#MEMORY table
$html += @"
    </table>
    <h2 style="color: #DF9229;">MEMORY Utilization Report</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>PageLifeExpectancy</th>
            <th>Memory Grants Pending</th>
            <th>Min Server Memory_GB</th>
            <th>Max Server Memory_GB</th>
            <th>Total Server Memory_GB</th>
            <th>Target ServerMemory_GB</th>
            <th>Ratio_Total_Target</th>
            <th>Total MemoryUsed_MB</th>
            <th>BufferPool Allocated_MB</th>
            <th>Comments</th>  
        </tr>
"@

foreach ($row in $memdata) {
switch ($row.Comments) {
        "Memory Utilisation is HIGH" { $statusColor = "red" }
        "Memory Utilisation looks GOOD" { $statusColor = "lightgreen" }
                       }
    $html += "<tr>"    
    $html += "<td>$($row.Server_Name)</td>"
    $html += "<td style='text-align: right;'>$($row.PageLifeExpectancy)</td>"
    $html += "<td style='text-align: right;'>$($row.MemoryGrantsPending)</td>"
    $html += "<td style='text-align: right;'>$($row.MinServerMemory_GB)</td>"
    $html += "<td style='text-align: right;'>$($row.MaxServerMemory_GB)</td>"
    $html += "<td style='text-align: right;'>$($row.TotalServerMemory_GB)</td>"
    $html += "<td style='text-align: right;'>$($row.TargetServerMemory_GB)</td>"
    $html += "<td style='text-align: right;'>$($row.Ratio_Total_Target)</td>"
    $html += "<td style='text-align: right;'>$($row.TotalMemoryUsed_MB)</td>"
    $html += "<td style='text-align: right;'>$($row.BufferPoolAllocated_MB)</td>"
    $html += "<td style='background-color: $statusColor;'>$($row.Comments)</td>"
    #$html += "<td>$($row.Comments)</td>"       
    $html += "</tr>"
}
#									

#Errorlog Table
$html += @"
    </table>
    <h2 style="color: #DF9229;">ErrorLog Report</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>Date</th>
            <th>ProcessInfo</th>
            <th>Comments</th>
        </tr>
"@

foreach ($row in $errorlogdata) {
switch ($row.ProcessInfo) {
        "Fail"     { $statusColor = "red" }
        "Check did not find out anything major"  { $statusColor = "lightgreen" }
                       }
    $html += "<tr>"    
    $html += "<td>$($row.servername)</td>"
    $html += "<td style='text-align: right;'>$($row.Date)</td>"
    $html += "<td style='background-color: $statusColor; text-align: center;'>$($row.ProcessInfo)</td>"
    $html += "<td style='text-align: center;'>$($row.Comments)</td>"          
    $html += "</tr>"
}
#servername	Date	ProcessInfo	Comments

#Deadlock Table
$html += @"
    </table>
    <h2 style="color: #DF9229;">Blocking/Deadlock Report</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>HostName</th>
            <th>Blocking</th>
            <th>DbName</th>
            <th>LoginName</th>
            <th>LeadingBlocker</th>
            <th>WaitingSpid</th>
            <th>Command</th>
            <th>LoginTime</th>
            <th>LastRequestStart</th>
            <th>LastRequestEnd</th>
            <th>BlockingChain</th>
            <th>leading statement</th>
            <th>waiting query</th>
            <th>SessionInfo</th>  
        </tr>
"@

foreach ($row in $deadlockdata) {
switch ($row.Blocking) {
        "Blocking Detected"     { $statusColor = "red" }
        "No Blocking Detected"  { $statusColor = "lightgreen" }
                       }
    $html += "<tr>"
    $html += "<td>$($row.servername)</td>"    
    $html += "<td>$($row.HostName)</td>"
    $html += "<td style='background-color: $statusColor;'>$($row.Blocking)</td>"
    $html += "<td style='text-align: right;'>$($row.DbName)</td>"
    $html += "<td style='text-align: right;'>$($row.LoginName)</td>"
    $html += "<td style='text-align: right;'>$($row.LeadingBlocker)</td>"
    $html += "<td style='text-align: right;'>$($row.WaitingSpid)</td>"
    $html += "<td style='text-align: right;'>$($row.Command)</td>"
    $html += "<td style='text-align: right;'>$($row.LoginTime)</td>"
    $html += "<td style='text-align: right;'>$($row.LastRequestStart)</td>"
    $html += "<td style='text-align: right;'>$($row.LastRequestEnd)</td>"
    $html += "<td style='text-align: right;'>$($row.BlockingChain)</td>"
    $html += "<td style='text-align: right;'>$($row.leading_statement)</td>"
    $html += "<td style='text-align: right;'>$($row.waiting_query)</td>"
    $html += "<td style='text-align: right;'>$($row.SessionInfo)</td>"          
    $html += "</tr>"
}

#Disk Table
$html += @"
    </table>
    <h2 style="color: #DF9229;">Disk Report</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>Disk</th>
            <th>Total Size GB</th>
            <th>Available Size GB</th>
            <th>SpaceFree</th>
            <th>Status</th>                      					
        </tr>
"@

foreach ($row in $diskdata) {
switch ($row.Status) {
        "Critical" { $statusColor = "red" }
        "Offline" { $statusColor = "red" }
        "Stopped" { $statusColor = "red" }
        "Warning" { $statusColor = "yellow" }
        "Good" { $statusColor = "green" }
        "Healthy" { $statusColor = "lightgreen" }
        "Online" { $statusColor = "lightgreen" }
    }
    $html += "<tr>"
    $html += "<td>$($row.server_name)</td>"
    $html += "<td>$($row.DiskMountPoint)</td>"
    $html += "<td style='text-align: right;'>$($row.TotalSizeGB)</td>"
    $html += "<td style='text-align: right;'>$($row.AvailableSizeGB)</td>"
    $html += "<td style='text-align: right;'>$($row.SpaceFree)</td>"
    $html += "<td style='background-color: $statusColor; text-align: center;'>$($row.Status)</td>"
    #$html += "<td style='background-color: $($row.statusColor); text-align: center;'>$($row.Status)</td>"
    #$html += "<td style='background-color: $statusColor;'>$($row.Status)</td>" 
    $html += "</tr>"
}

#Temp Data
$html += @"
</table>
<body>
    <h2 style="color: #DF9229;">TempDB Utilisation Status</h2>
    <table>
        <tr>
            <th>Server</th>
            <th>Database Name</th>
            <th>FilesizeMB</th>
            <th>Available SpaceMB</th>
            <th>Percentfull</th>
            <th>status</th>
            
        </tr>
"@

foreach ($row in $tempdata) {
switch ($row.Status) {
        "Tempdb Running OUTOF Threshold" { $statusColor = "red" }
        "Tempdb Running Within Threshold" { $statusColor = "lightgreen" }
        
    }
    $html += "<tr>"
    $html += "<td>$($row.servername)</td>"
    $html += "<td style='text-align: center;'>$($row.databasename)</td>"
    $html += "<td style='text-align: right;'>$($row.filesizeMB)</td>"
    $html += "<td style='text-align: right;'>$($row.availableSpaceMB)</td>"
    $html += "<td style='text-align: center;'>$($row.percentfull)</td>"
    $html += "<td style='background-color: $statusColor; text-align: center;'>$($row.status)</td>"    
    $html += "</tr>"
}

#Newly Created Jobs
$html += @"
    </table>
    <h2 style="color: #DF9229;">SQL Server New Job Creation Report</h2>
    <table>
        <tr>
            <th>job_name</th>
            <th>date_created</th>
            <th>job_status</th>
            <th>last_run_status</th>
            <th>last_run_date</th>
            <th>last_run_duration_in_minutes</th>
            <th>schedule_name</th>
            <th>schedule_enabled</th>                     					
        </tr>
"@
#job_name	date_created	job_status	last_run_status	last_run_date	last_run_duration_in_minutes	schedule_name	schedule_enabled
foreach ($row in $NewlyCretedJobs) {
switch ($row.LastRunOutcome) {
        "Critical" { $statusColor = "red" }
        "Failed"   { $statusColor = "red" }
        "Stopped"  { $statusColor = "red" }
        "Warning"  { $statusColor = "yellow" }
        "Good"     { $statusColor = "green" }
        "Running"  { $statusColor = "lightgreen" }
        "Online"   { $statusColor = "lightgreen" }
                     }
    $html += "<tr>"
    $html += "<td>$($row.job_name)</td>"
    $html += "<td>$($row.date_created)</td>"
    $html += "<td>$($row.job_status)</td>"
    $html += "<td>$($row.last_run_status)</td>"
    $html += "<td>$($row.last_run_date)</td>"
    $html += "<td>$($row.last_run_duration_in_minutes)</td>"
    $html += "<td>$($row.schedule_name)</td>"
    $html += "<td>$($row.schedule_enabled)</td>"
    #$html += "<td>$($row.NextRunTime)</td>"
    #$html += "<td style='background-color: $statusColor; text-align: center;'>$($row.LastRunOutcome)</td>"
    $html += "</tr>"
}

#JOB information
$html += @"
    </table>
    <h2 style="color: #DF9229;">SQL Server Job Report</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>JobName</th>
            <th>JobOwner</th>
            <th>Schedule Name</th>
            <th>IsEnabled</th>
            <th>Frequency</th>
            <th>Interval</th>
            <th>Time</th>
            <th>NextRun Time</th>
            <th>LastRun Status</th>                      					
        </tr>
"@

foreach ($row in $job) {
switch ($row.LastRunOutcome) {
        "Critical" { $statusColor = "red" }
        "Failed"   { $statusColor = "red" }
        "Stopped"  { $statusColor = "red" }
        "Warning"  { $statusColor = "yellow" }
        "Good"     { $statusColor = "green" }
        "Running"  { $statusColor = "lightgreen" }
        "Online"   { $statusColor = "lightgreen" }
                     }
    $html += "<tr>"
    $html += "<td>$($row.Server_Name)</td>"
    $html += "<td>$($row.JobName)</td>"
    $html += "<td>$($row.JobOwner)</td>"
    $html += "<td>$($row.ScheduleName)</td>"
    $html += "<td>$($row.IsEnabled)</td>"
    $html += "<td>$($row.Frequency)</td>"
    $html += "<td>$($row.Interval)</td>"
    $html += "<td>$($row.Time)</td>"
    $html += "<td>$($row.NextRunTime)</td>"
    $html += "<td style='background-color: $statusColor; text-align: center;'>$($row.LastRunOutcome)</td>"
    #$html += "<td>$($row.Status)</td>"    
    $html += "</tr>"
}			


#SYSADMIN Account list
$html += @"
    </table>
    <h2 style="color: #DF9229;">List of SYSADMIN accounts</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>Login Name</th>
            <th>Type</th>
            <th>SQL/Windows Login</th>
            <th>Created Date</th>
            <th>Update Date</th>
                  
        </tr>
"@
					
foreach ($row in $sysadmin) {
    $html += "<tr>"
    $html += "<td>$($row.Server_Name)</td>"
    $html += "<td>$($row.loginname)</td>"
    $html += "<td>$($row.type)</td>"
    $html += "<td>$($row.type_desc)</td>"
    $html += "<td>$($row.created)</td>"
    $html += "<td>$($row.update)</td>"          
    $html += "</tr>"
}

#Linkedserver Table
$html += @"
    </table>
    <h2 style="color: #DF9229;">Linkedserver Report</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>server id</th>
            <th>Name</th>
            <th>Product</th>
            <th>Provider</th>
            <th>data_source</th>
            <th>Location</th>
            <th>Provider string</th>
        </tr>
"@

foreach ($row in $linkedserverData) {
    $html += "<tr>"
    $html += "<td>$($row.servername)</td>"
    $html += "<td>$($row.server_id)</td>"
    $html += "<td>$($row.name)</td>"
    $html += "<td>$($row.product)</td>"
    $html += "<td>$($row.provider)</td>"
    $html += "<td>$($row.data_source)</td>"
    $html += "<td>$($row.location)</td>"
    $html += "<td>$($row.provider_string)</td>"
    $html += "</tr>"
}

#Backup table
$html += @"
    </table>
    <h2 style="color: #DF9229;">Backup Report</h2>
    <table>
        <tr>
            <th>Server Name</th>
            <th>Database Name</th>
            <th>RecoveryModel</th>
            <th>DBStatus</th>
            <th>LastFullBackup</th>
            <th>LastDiffBackup</th>
            <th>LastLogBackup</th>
        </tr>
"@

foreach ($row in $backupData) {
    $html += "<tr>"
    $html += "<td>$($row.Server_name)</td>"
    $html += "<td>$($row.DBName)</td>"
    $html += "<td>$($row.RecoveryModel)</td>"
    $html += "<td>$($row.DBStatus)</td>"
    $html += "<td>$($row.LastFullBackup)</td>"
    $html += "<td>$($row.LastDiffBackup)</td>"
    $html += "<td>$($row.LastLogBackup)</td>"
    $html += "</tr>"
}


$html += @"
</table>
</body>
</html>
"@

# Save HTML to file
$outputFile = "G:\Database_Healthcheck_Report.html"
$html | Out-File -FilePath $outputFile -Encoding UTF8
