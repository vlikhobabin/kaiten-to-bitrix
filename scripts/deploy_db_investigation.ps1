# Deploy script for DB investigation
# Deploys investigate_db_structure.py to VPS server

# Function to read environment variables from .env or env.txt file
function Read-EnvFile {
    param([string]$FilePath)
    
    $envVars = @{}
    if (Test-Path $FilePath) {
        Get-Content $FilePath | Where-Object { $_ -notmatch '^#' -and $_ -match '=' } | ForEach-Object {
            $key, $value = $_ -split '=', 2
            # Remove quotes and trim whitespace
            $value = $value.Trim().Trim('"').Trim("'")
            $envVars[$key] = $value
        }
    }
    return $envVars
}

# Load environment variables from .env or env.txt
$envFile = if (Test-Path ".env") { ".env" } else { "env.txt" }
$envVars = Read-EnvFile -FilePath $envFile

# SSH Settings from environment variables
$ServerHost = $envVars["SSH_HOST"]
$ServerUser = $envVars["SSH_USER"]  
$PrivateKeyPath = $envVars["SSH_KEY_PATH_PUTTY"]
$LocalScriptPath = "scripts\investigate_db_structure.py"
$RemoteScriptPath = "/root/investigate_db_structure.py"

# Validate required settings
if (-not $ServerHost -or -not $ServerUser -or -not $PrivateKeyPath) {
    Write-Host "ERROR: Missing required SSH settings in $envFile file!" -ForegroundColor Red
    Write-Host "Required: SSH_HOST, SSH_USER, SSH_KEY_PATH_PUTTY" -ForegroundColor Red
    exit 1
}

Write-Host "Deploying DB investigation script to VPS server..." -ForegroundColor Yellow
Write-Host "Local file: $LocalScriptPath" -ForegroundColor White
Write-Host "Remote path: $RemoteScriptPath" -ForegroundColor White
Write-Host ""

# Check if local file exists
if (-not (Test-Path $LocalScriptPath)) {
    Write-Host "ERROR: File $LocalScriptPath not found!" -ForegroundColor Red
    exit 1
}

# Check if private key exists
if (-not (Test-Path $PrivateKeyPath)) {
    Write-Host "ERROR: Private key $PrivateKeyPath not found!" -ForegroundColor Red
    exit 1
}

try {
    Write-Host "Copying file to server..." -ForegroundColor Yellow
    
    # Copy file using pscp
    $pscpCommand = "pscp.exe -i `"$PrivateKeyPath`" `"$LocalScriptPath`" ${ServerUser}@${ServerHost}:$RemoteScriptPath"
    Write-Host "Command: $pscpCommand" -ForegroundColor Gray
    
    Invoke-Expression $pscpCommand
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "SUCCESS: File copied successfully!" -ForegroundColor Green
    } else {
        Write-Host "ERROR: Failed to copy file (exit code: $LASTEXITCODE)" -ForegroundColor Red
        exit 1
    }
    
    Write-Host ""
    Write-Host "Setting permissions on server..." -ForegroundColor Yellow
    
    # Set execute permissions
    $sshCommand = "plink.exe -i `"$PrivateKeyPath`" ${ServerUser}@${ServerHost} `"chmod +x $RemoteScriptPath`""
    Write-Host "Command: $sshCommand" -ForegroundColor Gray
    
    Invoke-Expression $sshCommand
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "SUCCESS: Permissions set!" -ForegroundColor Green
    } else {
        Write-Host "WARNING: Could not set permissions" -ForegroundColor Yellow
    }
    
    Write-Host ""
    Write-Host "DEPLOYMENT COMPLETED SUCCESSFULLY!" -ForegroundColor Green
    
    # Ask if user wants to run the script immediately
    $runNow = Read-Host "Run DB investigation script now? (y/n)"
    
    if ($runNow -eq "y" -or $runNow -eq "Y") {
        Write-Host ""
        Write-Host "Running DB investigation script..." -ForegroundColor Yellow
        Write-Host "=" * 60 -ForegroundColor Blue
        
        $runCommand = "plink.exe -i `"$PrivateKeyPath`" ${ServerUser}@${ServerHost} `"python3 $RemoteScriptPath`""
        Invoke-Expression $runCommand
        
        Write-Host ""
        Write-Host "=" * 60 -ForegroundColor Blue
        Write-Host "DB investigation completed!" -ForegroundColor Green
    } else {
        Write-Host ""
        Write-Host "To run the script manually:" -ForegroundColor Yellow
        Write-Host "  ssh $ServerUser@$ServerHost" -ForegroundColor Gray
        Write-Host "  python3 $RemoteScriptPath" -ForegroundColor Gray
    }
    
} catch {
    Write-Host "CRITICAL ERROR: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
} 