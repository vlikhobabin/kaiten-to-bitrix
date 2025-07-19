# Simple deployment script for update_comment_dates.py
# No special characters to avoid encoding issues

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
$LocalScriptPath = "scripts\update_comment_dates.py"
$RemoteScriptPath = $envVars["VPS_SCRIPT_PATH"]

# Validate required settings
if (-not $ServerHost -or -not $ServerUser -or -not $PrivateKeyPath -or -not $RemoteScriptPath) {
    Write-Host "ERROR: Missing required SSH settings in $envFile file!" -ForegroundColor Red
    Write-Host "Required: SSH_HOST, SSH_USER, SSH_KEY_PATH_PUTTY, VPS_SCRIPT_PATH" -ForegroundColor Red
    exit 1
}

Write-Host "Deploying update_comment_dates.py to VPS server..."
Write-Host "Local file: $LocalScriptPath"
Write-Host "Remote path: $RemoteScriptPath"
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
    Write-Host "Command: $pscpCommand"
    
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
    Write-Host "Command: $sshCommand"
    
    Invoke-Expression $sshCommand
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "SUCCESS: Permissions set!" -ForegroundColor Green
    } else {
        Write-Host "WARNING: Could not set permissions" -ForegroundColor Yellow
    }
    
    Write-Host ""
    Write-Host "DEPLOYMENT COMPLETED SUCCESSFULLY!" -ForegroundColor Green
    Write-Host "To use the script on server:"
    Write-Host "  ssh bitrix"
    Write-Host "  python3 $RemoteScriptPath '{\"comment_id\": \"2025-07-08 14:22:00\"}'"
    
} catch {
    Write-Host "CRITICAL ERROR: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
} 