# Function definitions
function Load-Required-Packages {

    #Requires
    try { $nugetPkg = (Get-Package Oracle.ManagedDataAccess -ProviderName NuGet -MinimumVersion "19.3.1" -ErrorAction Stop -Debug:$false).Source }
    catch [System.Exception] {
        if($_.CategoryInfo.Category -eq "ObjectNotFound") {
            # Register NuGet.org as a package source if it is missing.
            try { Get-PackageSource -Name "nuget.org" -ProviderName NuGet -Debug:$false -ErrorAction Stop }
            catch [System.Exception] {
                if($_.CategoryInfo.Category -eq "ObjectNotFound") {
                    Register-PackageSource -Name "nuget.org" -Location "https://www.nuget.org/api/v2/" -ProviderName NuGet -Debug:$false
                }
                else { throw $_ }
            }

            # Install Oracle drivers.
            $pkg = (Install-Package Oracle.ManagedDataAccess -ProviderName NuGet -MinimumVersion "19.3.1" -Verbose -Scope CurrentUser -Force -Debug:$false).Payload.Directories[0]
            $nugetPkg = Join-Path -Path $pkg.Location -ChildPath $pkg.Name
            Remove-Variable pkg
        }
        else { throw $_ }
    }

    #Load required types and modules
    Add-Type -Path (Get-ChildItem (Split-Path ($nugetPkg) -Parent) -Filter "Oracle.ManagedDataAccess.dll" -Recurse -File)[0].FullName

}

function Get-OraTnsAdminEntries {

    $result = ""

    $tnsEntries = @{}

    [System.IO.FileInfo]$file = Get-Item $global:Database.OraTnsnames

    [string]$data = gc $file.FullName | select-string -Pattern '#' -NotMatch    

    $lines =  $data.Replace("`n","").Replace(" ","").Replace("`t","").Replace(")))(","))xXxX(").Replace(")))",")))`n").Replace("))xXxX(",")))(")

    foreach ($line in $lines.Split("`n")) {

        if ($line.Trim().Length -gt 0) {

            $key = $line.Substring(0, $line.IndexOf("="))
            $value = $line.Substring($line.IndexOf("=") + 1, $line.Length - $line.IndexOf("=") - 1)

            $tnsEntries.Add($key, $value)

        }

    }    

    $result = $tnsEntries[$global:Database.Alias]

    return $result

}

function Load-Config-File {

    $configPath = "config.json"
    $configPath = Join-Path $PSScriptRoot $configPath
    $configData = Get-Content $configPath | ConvertFrom-JSON

    $global:Database = $configData.Database
    $global:OutputFolder = $configData.Folders.Output
    $global:Credentials = $configData.Credentials

}

function Check-SelectCatalogRole-Granted {

    param( 
        $connection,
        $username
    )    
    
    begin {    

        # DO NOT terminate it with semicolon, it does not work with
        $queryGrantedRole = "SELECT COUNT(*) AS GRANTED FROM User_Role_Privs WHERE GRANTED_ROLE = 'SELECT_CATALOG_ROLE' AND USERNAME = '{0}'"

    }

    process {

        $result = $false

        #Create command object
        $cmd = $connection.CreateCommand();
        $cmd.CommandText = %{ $queryGrantedRole -f $username }

        #Create data adapter object
        $da = New-Object Oracle.ManagedDataAccess.Client.OracleDataAdapter($cmd)

        $dt = New-Object System.Data.DataTable
        [void]$da.Fill($dt)

        $ci = $dt.Columns | ForEach-Object {$t = @{}} {$t[$_.ColumnName] = $_.Ordinal} {return $t}

        if ($dt.Rows.Count -gt 0) {
            if ($dt.Rows[0][$ci.GRANTED] -gt 0) {
                $result = $true            
            }
        }

        return $result

    }

    end {}

}

function Fetch-Chunk {
    
    param (
        $connection,
        $objecttype,
        $objectname,
        $owner        
    )

    begin {

        # Set maximum buffer size for LOB data type
        $chunkAmount = 2000

        # DO NOT terminate it with semicolon, it does not work with
        $queryLOBSize = "SELECT dbms_lob.getlength(t.DDL_SCRIPT) AS CHUNKSIZE FROM (SELECT dbms_metadata.get_ddl('{0}', '{1}', '{2}') AS DDL_SCRIPT FROM DUAL) t"
        $queryLOBChunk = "SELECT dbms_lob.substr(t.DDL_SCRIPT, {3}, {4}) AS CHUNKBODY FROM (SELECT dbms_metadata.get_ddl('{0}', '{1}', '{2}') AS DDL_SCRIPT FROM DUAL) t"

    }

    process {

        #Create command object
        $cmdLOBSize = $connection.CreateCommand();
        $cmdLOBSize.CommandText = %{ $queryLOBSize -f $objecttype.Replace(" ", "_"), $objectname, $owner }

        #Create data adapter object
        $daSize = New-Object Oracle.ManagedDataAccess.Client.OracleDataAdapter($cmdLOBSize)

        $dtSize = New-Object System.Data.DataTable
        [void]$daSize.Fill($dtSize)

        $chunkSize = $dtSize.Rows[0][0]    

        $chunkOffset = 1
        $body = ""

        while (($chunkSize - $chunkOffset - 1) -ge 0) {

            Write-Host ( "Getting script {0}.{1}(type {2})...{3}/{4}" -f $owner, $objectname, $objecttype, ($chunkOffset - 1), $chunkSize )

            $cmdLOBScript = $connection.CreateCommand();
            $cmdLOBScript.CommandText = %{ $queryLOBChunk -f $objecttype.Replace(" ", "_"), $objectname, $owner, $chunkAmount, $chunkOffset }

            #Create data adapter object
            $daScript = New-Object Oracle.ManagedDataAccess.Client.OracleDataAdapter($cmdLOBScript)

            $dtScript = New-Object System.Data.DataTable
            [void]$daScript.Fill($dtScript)

            $body += $dtScript.Rows[0][0]    

            $chunkOffset += $chunkAmount        

        }

        return $body

    }

    end {}

}


function Retrieve-Folder-Name {
    
    param (
        $connection,
        $owner,
        $objecttype,
        $objectname
    )

    begin {

        $queryTriggerType = "SELECT BASE_OBJECT_TYPE FROM DBA_TRIGGERS WHERE OWNER = '{0}' AND TRIGGER_NAME = '{1}'"
        $queryObjectTable = "SELECT COUNT(*) AS ISOBJECTTABLE FROM DBA_OBJECT_TABLES WHERE OWNER = '{0}' AND TABLE_NAME = '{1}'"
        $queryXmlTable = "SELECT COUNT(*) AS ISXMLTABLE FROM DBA_XML_TABLES WHERE OWNER = '{0}' AND TABLE_NAME = '{1}'"
        $queryXmlView = "SELECT * FROM DBA_XML_VIEWS WHERE OWNER = '{0}' AND VIEW_NAME = '{1}'"

        # Mapping object type to folder name 
        $objectMapping = @{ 
            "FUNCTION" = "Functions"; 
            "VIEW" = "Views\Relational Views";
            "MATERIALIZED VIEW" = "Views\Materialized Views";
            "VIEW_OBJECT" = "Views\Object Views";
            "VIEW XML" = "Views\XML Views";
            "PROCEDURE" = "Procedures";
            "INDEX" = "Indexes";
            "PACKAGE" = "Packages";
            "PACKAGE BODY" = "Packages";
            "SYNONYM" = "Synonyms";
            "SEQUENCE" = "Sequences";
            "TRIGGER TABLE" = "Triggers\Table Triggers";
            "TRIGGER VIEW" = "Triggers\View Triggers";
            "TRIGGER SCHEMA" = "Triggers\Schema Triggers";
            "TRIGGER DATABASE" = "Triggers\Database Triggers";
            "TABLE" = "Tables\Relational Tables";
            "TABLE OBJECT" = "Tables\Object Tables";
            "TABLE XML" = "Tables\XML Tables";
        }

    }

    process {

        $result = $objecttype

        if ($objecttype -like "TABLE") {

            #Create command object
            $command = $connection.CreateCommand()
            $command.CommandText = %{ $queryObjectTable -f $owner,  $objectname }

            #Create data set object
            $da = New-Object Oracle.ManagedDataAccess.Client.OracleDataAdapter($command)

            $dt = New-Object System.Data.DataTable
            [void]$da.Fill($dt)

            if ($dt.Rows.Count -gt 0) {
                if ($dt.Rows[0][0] -gt 0) {
                    $objecttype = $objecttype + " OBJECT"
                }
            }

            $command.CommandText = %{ $queryXmlTable -f $owner,  $objectname }

            #Create data set object
            $da = New-Object Oracle.ManagedDataAccess.Client.OracleDataAdapter($command)

            $dt = New-Object System.Data.DataTable
            [void]$da.Fill($dt)

            if ($dt.Rows.Count -gt 0) {
                if ($dt.Rows[0][0] -gt 0) {
                    $objecttype = $objecttype + " XML"
                }
            }            

        }
        elseif ($objecttype -like "VIEW") {

            #Create command object
            $command = $connection.CreateCommand()
            $command.CommandText = %{ $queryXmlView -f $owner,  $objectname }

            #Create data set object
            $da = New-Object Oracle.ManagedDataAccess.Client.OracleDataAdapter($command)

            $dt = New-Object System.Data.DataTable
            [void]$da.Fill($dt)

            if ($dt.Rows.Count -gt 0) {
                if ($dt.Rows[0][0] -gt 0) {
                    $objecttype = $objecttype + " XML"
                }
            } 

        }
        elseif ($objecttype -like "TRIGGER") {

            #Create command object
            $command = $connection.CreateCommand()
            $command.CommandText = %{ $queryTriggerType -f $owner,  $objectname }

            #Create data set object
            $da = New-Object Oracle.ManagedDataAccess.Client.OracleDataAdapter($command)

            $dt = New-Object System.Data.DataTable
            [void]$da.Fill($dt)

            $triggerType = $dt.Rows[0][0]

            $objecttype = $objecttype + " " + $triggerType

        }

        $result = $objectMapping[$objecttype]

        return $result

    }

    end {}

}

function Store-Script {

    param (
        $body,
        $foldername,
        $objectname,
        $owner        
    )

    begin {}
    
    process {

        $filePath = %{ "{0}\{1}\{2}.{3}.sql" -f $global:OutputFolder, $foldername, $owner, $objectname }

        New-Item -Force $filePath | Out-Null
        Add-Content $filePath $body -Encoding utf8 | Out-Null

    }

    end {}

}

#Load and init required packages
Load-Required-Packages

#Load configuraion file
Load-Config-File

#The Oracle DataSource as you would compile it in your TNSNAMES.ORA
$dataSource = Get-OraTnsAdminEntries

$username = $global:Credentials.Username
$password = $global:Credentials.Password

# DO NOT terminate it with semicolon, it does not work with
$queryStatement = "SELECT * FROM dba_objects WHERE OWNER IN ({0}) AND OBJECT_TYPE IN ({1}) ORDER BY OBJECT_TYPE, OBJECT_NAME"

#Create the connection string
$connectionString = 'User Id=' + $username + ';Password=' + $password + ';Data Source=' + $dataSource

try {

    #Create connection object
    $connection = New-Object Oracle.ManagedDataAccess.Client.OracleConnection($connectionString)    

    $connection.Open();
    Write-Host ("Connected to database: {0} – running on host: {1} – Servicename: {2} – Serverversion: {3}" -f $connection.DatabaseName, $connection.HostName, $connection.ServiceName, $connection.ServerVersion) -ForegroundColor Cyan -BackgroundColor Black

    $roleIsGranted = Check-SelectCatalogRole-Granted $connection $username
    if ($roleIsGranted -eq $False) {
        Write-Host ("SELECT_CATALOG_ROLE is not granted to user {0} or SQL command failed." -f $username) -ForegroundColor Red
        return
    }

    #Prepare collection of schemes and object types
    $schemes = "'" + ($global:Database.Schemes -join "', '") + "'"
    $types = "'" + ($global:Database.Types -join "', '") + "'"

    #Create command object
    $command = $connection.CreateCommand()
    $command.CommandText = %{ $queryStatement -f $schemes, $types }

    #Create data set object
    $dataAdapter = New-Object Oracle.ManagedDataAccess.Client.OracleDataAdapter($command)

    $resultSet = New-Object System.Data.DataTable
    $dataAdapter.Fill($resultSet)

    $ci = $resultSet.Columns | ForEach-Object {$t = @{}} {$t[$_.ColumnName] = $_.Ordinal} {return $t}

    foreach ($row in $resultSet.Rows) {        

        Write-Host ("Retrieve DLL script for object (OWNER = {0}; 0BJECT_NAME = {1}; OBJECT_TYPE = {2};)" -f $row[$ci.OWNER], $row[$ci.OBJECT_NAME], $row[$ci.OBJECT_TYPE])

        try {

            $script = Fetch-Chunk $connection $row[$ci.OBJECT_TYPE] $row[$ci.OBJECT_NAME] $row[$ci.OWNER]

            $foldername = Retrieve-Folder-Name @connection $row[$ci.OWNER] $row[$ci.OBJECT_TYPE] $row[$ci.OBJECT_NAME]

            Store-Script $script $foldername $row[$ci.OBJECT_NAME] $row[$ci.OWNER]

        } 
        catch {
        
            Write-Host ("ERROR: $_") -ForegroundColor Red
                    
        }

    }

} catch {

    Write-Host ("ERROR: $_") -ForegroundColor Red

}
finally {

    if ($connection.State -eq 'Open') { 
        $connection.close() 
    }
    $connection.Dispose()
    Write-Host "Disconnected from database" -ForegroundColor Cyan -BackgroundColor Black

}
