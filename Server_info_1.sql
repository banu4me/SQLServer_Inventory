--SELECT SYSDATETIME() as 'ReportGenDate'
drop table #output;
drop table #diskinfo_t;

DECLARE @build varchar(15);
Declare @value nvarchar(15);
declare @osversion nvarchar(100);
DECLARE @AGPrimarynode varchar(15);
DECLARE @AGSecnode varchar(15);
DECLARE @Listner varchar(15);
DECLARE @DBNames VARCHAR(MAX);
DECLARE @Replica_names VARCHAR(MAX);
Declare @noofdbs nvarchar(15);
Declare @TotDbsize bigint;
Declare @TotPhysicalMemory nvarchar(20);
Declare @TotCPU nvarchar(20);
Declare @maxmemory sql_variant;
Declare @TDE nvarchar(1000);
Declare @Diskinfo_report nvarchar(1000);
Declare @collation nvarchar(50);
Declare @compatabilitylevel nvarchar(200);
Declare @AGgroup nvarchar(200);

--
declare @svrName varchar(255)
declare @sql varchar(400)
Declare @diskinfo nvarchar(200)
--by default it will take the current server name, we can the set the server name as well
set @svrName = @@SERVERNAME
set @sql = 'powershell.exe -c "Get-WmiObject -ComputerName ' + QUOTENAME(@svrName,'''') + ' -Class Win32_Volume -Filter ''DriveType = 3'' | select name,capacity,freespace | foreach{$_.name+''|''+$_.capacity/1048576+''%''+$_.freespace/1048576+''*''}"'
--creating a temporary table
CREATE TABLE #output
(line varchar(255))
--EXEC @diskinfo =xp_cmdshell @sql
--inserting disk name, total space and free space value in to temporary table
insert #output
EXEC xp_cmdshell @sql
select rtrim(ltrim(SUBSTRING(line,1,CHARINDEX('|',line) -1))) as Drive_Name
   ,round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('|',line)+1,
   (CHARINDEX('%',line) -1)-CHARINDEX('|',line)) )) as Float)/1024,0) as 'Drive_capacity_GB'
 into #diskinfo_t from #output
where line like '[A-Z][:]%'
order by Drive_Name
--script to drop the temporary table
--drop table #output
--declare @Diskinfo_report nvarchar (1000);
Declare @table1 table (id int , value varchar(1000));
insert into @table1 (id,value)
select 1,drive_name+'('+cast(Drive_capacity_GB as varchar(10)) +')' from #diskinfo_t
SELECT  @Diskinfo_report=(select distinct STUFF((SELECT ', ' + CAST(Value AS VARCHAR(10)) [text()] FROM @Table1 
WHERE ID = t.ID FOR XML PATH('tr'), TYPE).value('.','NVARCHAR(MAX)'),1,2,' ') List_Output
FROM @Table1 t ) 
--select @Diskinfo_report  ;

select @TDE=name from sys.certificates where issuer_name like '%TDE%';
SELECT @TotCPU=cpu_count FROM sys.dm_os_sys_info;
SELECT @TotPhysicalMemory=FORMAT((physical_memory_kb /1024.00/1024.00) ,'N2') FROM sys.dm_os_sys_info;
select @TotDbsize=sum((mFiles.size)*8/1024)/1024 from SYS.MASTER_FILES mFiles INNER JOIN SYS.DATABASES dbs
      ON dbs.DATABASE_ID = mFiles.DATABASE_ID WHERE dbs.DATABASE_ID > 4;
--DB Names
SELECT @DBNames = COALESCE(@DBNames+', ' ,'') +name
FROM sys.databases where database_id>4;
--Compatability level
--select @compatabilitylevel=coalesce( @compatabilitylevel +', ' ,'') +name +compatibility_level , version_name = 
--CASE compatibility_level
--    WHEN 65  THEN 'SQL Server 6.5'
--    WHEN 70  THEN 'SQL Server 7.0'
--    WHEN 80  THEN 'SQL Server 2000'
--    WHEN 90  THEN 'SQL Server 2005'
--    WHEN 100 THEN 'SQL Server 2008/R2'
--    WHEN 110 THEN 'SQL Server 2012'
--    WHEN 120 THEN 'SQL Server 2014'
--    WHEN 130 THEN 'SQL Server 2016'
--    WHEN 140 THEN 'SQL Server 2017'
--    WHEN 150 THEN 'SQL Server 2019'
--    WHEN 160 THEN 'SQL Server 2022'
--    ELSE 'new unknown - '+CONVERT(varchar(10),compatibility_level)
--END from sys.databases


select @noofdbs=count(*) from sys.databases where database_id>4;
SELECT @Replica_names = COALESCE(@replica_names+', ' ,'') +cs.replica_server_name
FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_group_states ags ON ag.group_id = ags.group_id
JOIN sys.dm_hadr_availability_replica_cluster_states cs ON ags.group_id = cs.group_id 
JOIN sys.availability_replicas ar ON ar.replica_id = cs.replica_id 
JOIN sys.dm_hadr_availability_replica_states rs  ON rs.replica_id = cs.replica_id 
LEFT JOIN sys.availability_group_listeners al ON ar.group_id = al.group_id

select @AGgroup=name from sys.availability_groups

set @Listner=(SELECT al.dns_name AS 'Listener'
   FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_group_states ags ON ag.group_id = ags.group_id
JOIN sys.dm_hadr_availability_replica_cluster_states cs ON ags.group_id = cs.group_id 
JOIN sys.availability_replicas ar ON ar.replica_id = cs.replica_id 
JOIN sys.dm_hadr_availability_replica_states rs  ON rs.replica_id = cs.replica_id 
LEFT JOIN sys.availability_group_listeners al ON ar.group_id = al.group_id where role_desc='PRIMARY')
set @build=(SELECT SUBSTRING(@@VERSION,CHARINDEX('build',@@VERSION,0),11) AS OSVersion)
set @value=(SELECT RIGHT(@build,5) AS OSVersion)
--print @value
SELECT @maxmemory=c.value FROM sys.configurations c WHERE c.[name] = 'max server memory (MB)';
select (SELECT SYSDATETIME()) as 'ReportGenDate',
		(select serverproperty('ComputerNamePhysicalNetBIOS')) as 'ServerName',
		(Select SERVERPROPERTY('MachineName')) as 'SQLV-Server',
		--@@SERVERNAME 'Server_Name' , 
		(SELECT @@servicename) as InstanceName,
		--SERVERPROPERTY('InstanceName') as Instance,
		--SERVERPROPERTY('ServerName') AS InstanceName,  
(select case @value 
WHEN '3790:' THEN 'Windows Server 2003'
WHEN '3790:' THEN 'Windows Server 2003 R2'
WHEN '6003:' THEN 'Windows Server 2008'
WHEN '7601:' THEN 'Windows Server 2008 R2'   
WHEN '9200:' THEN 'Windows Server 2012'   
WHEN '9600:' THEN 'Windows Server 2012 R2'   
WHEN '14393'  THEN 'Windows Server 2016'
WHEN '17763'  THEN 'Windows Server 2019'
END as Windows_OS_Name
FROM sys.dm_os_windows_info a) OS_Versi,
		  LEFT (@@VERSION, 35) as SQL_Version, 
          SERVERPROPERTY('Edition') as Edition,
		  SERVERPROPERTY('collation') AS SQLServerCollation,
		  @TotPhysicalMemory as 'Total_Physical_RAM_GB',
		  @maxmemory as SQL_Memory,
		  @TotCPU as 'No_Of_Processors',
		  @noofdbs as 'No of DBs',@TotDbsize as TOT_DBs_SIZE_GB,
		  @TDE as 'TDE',
          --SERVERPROPERTY('ProductVersion') AS ProductVersion,  
		  --SERVERPROPERTY('ProductLevel') as ProductLevel, /* RTM or SP1 etc*/
	      Case SERVERPROPERTY('IsClustered') when 1 then 'CLUSTERED' else
          'STANDALONE' end as ServerType,
		  CASE SERVERPROPERTY ('IsHadrEnabled') when 1 then 'AG' else
          'STANDALONE' end as 'AO STATUS',
		  (SELECT @Replica_names +', '+'Listner_Name:  '+@Listner+', '+'AGGroup:  '+@AGgroup) as AG_Details,(select @DBNames) as Database_Names,(select @Diskinfo_report) as Diskinfo
		  --@Diskinfo_report as Disk_info_GB into #temp

  go  	