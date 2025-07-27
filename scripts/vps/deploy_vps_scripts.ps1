# Unified VPS Deployment Script
# Deploys all VPS scripts to a single directory on the server

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
$PrivateKeyPath = $envVars["SSH_KEY_PATH"]

# VPS directory for all scripts
$VpsScriptsDir = "/root/kaiten-vps-scripts"

# Validate required settings
if (-not $ServerHost -or -not $ServerUser -or -not $PrivateKeyPath) {
    Write-Host "ERROR: Missing required SSH settings in $envFile file!" -ForegroundColor Red
    Write-Host "Required: SSH_HOST, SSH_USER, SSH_KEY_PATH" -ForegroundColor Red
    exit 1
}

Write-Host "UNIFIED VPS SCRIPTS DEPLOYMENT" -ForegroundColor Yellow
Write-Host "=" * 70 -ForegroundColor Blue
Write-Host "Target directory: $VpsScriptsDir" -ForegroundColor White
Write-Host ""

# Check if private key exists
if (-not (Test-Path $PrivateKeyPath)) {
    Write-Host "ERROR: Private key $PrivateKeyPath not found!" -ForegroundColor Red
    exit 1
}

# VPS Scripts to deploy
$VpsScripts = @(
    @{
        Local = "scripts\vps\create_custom_fields_on_vps.py"
        Remote = "$VpsScriptsDir/create_custom_fields_on_vps.py"
        Description = "Custom Fields Creation Script"
    },
    @{
        Local = "scripts\vps\update_comment_dates.py"
        Remote = "$VpsScriptsDir/update_comment_dates.py"
        Description = "Comment Dates Update Script"
    },
    @{
        Local = "scripts\vps\update_group_features.py"
        Remote = "$VpsScriptsDir/update_group_features.py"
        Description = "Group Features Management Script"
    }
)

try {
    Write-Host "[INFO] Creating VPS scripts directory..." -ForegroundColor Yellow
    
    $createDirCommand = "mkdir -p $VpsScriptsDir"
    
    $process = Start-Process -FilePath "ssh" -ArgumentList "-i", "`"$PrivateKeyPath`"", "${ServerUser}@${ServerHost}", $createDirCommand -Wait -PassThru -WindowStyle Hidden
    
    if ($process.ExitCode -eq 0) {
        Write-Host "[SUCCESS] VPS directory created: $VpsScriptsDir" -ForegroundColor Green
    } else {
        Write-Host "[ERROR] Failed to create VPS directory" -ForegroundColor Red
        exit 1
    }
    
    Write-Host ""
    Write-Host "[INFO] Deploying VPS scripts..." -ForegroundColor Yellow
    
    $SuccessfulDeployments = 0
    $TotalDeployments = $VpsScripts.Count
    
    foreach ($ScriptInfo in $VpsScripts) {
        $LocalPath = $ScriptInfo.Local
        $RemotePath = $ScriptInfo.Remote
        $Description = $ScriptInfo.Description
        
        Write-Host "  [FILE] $Description..." -ForegroundColor White
        Write-Host "     Local: $LocalPath" -ForegroundColor Gray
        Write-Host "     Remote: $RemotePath" -ForegroundColor Gray
        
        # Check if local file exists
        if (-not (Test-Path $LocalPath)) {
            Write-Host "     [ERROR] Local file not found!" -ForegroundColor Red
            continue
        }
        
        # Copy file using scp
        $process = Start-Process -FilePath "scp" -ArgumentList "-i", "`"$PrivateKeyPath`"", "`"$LocalPath`"", "${ServerUser}@${ServerHost}:$RemotePath" -Wait -PassThru -WindowStyle Hidden
        
        if ($process.ExitCode -eq 0) {
            Write-Host "     [SUCCESS] Deployed successfully" -ForegroundColor Green
            $SuccessfulDeployments++
        } else {
            Write-Host "     [ERROR] Deployment failed (exit code: $($process.ExitCode))" -ForegroundColor Red
        }
        
        Write-Host ""
    }
    
    Write-Host "[INFO] Setting permissions for all scripts..." -ForegroundColor Yellow
    
    $chmodCommand = "chmod +x $VpsScriptsDir/*.py && find $VpsScriptsDir -name '*.py' -exec chmod 755 {} \;"
    
    $process = Start-Process -FilePath "ssh" -ArgumentList "-i", "`"$PrivateKeyPath`"", "${ServerUser}@${ServerHost}", $chmodCommand -Wait -PassThru -WindowStyle Hidden
    
    if ($process.ExitCode -eq 0) {
        Write-Host "[SUCCESS] Permissions set successfully" -ForegroundColor Green
    } else {
        Write-Host "[WARNING] Could not set all permissions" -ForegroundColor Yellow
    }
    
    Write-Host ""
    Write-Host "DEPLOYMENT SUMMARY:" -ForegroundColor Blue
    Write-Host "=" * 50 -ForegroundColor Blue
    Write-Host "[SUCCESS] Deployed: $SuccessfulDeployments / $TotalDeployments scripts" -ForegroundColor Green
    Write-Host "VPS Directory: $VpsScriptsDir" -ForegroundColor White
    
    if ($SuccessfulDeployments -eq $TotalDeployments) {
        Write-Host "ALL VPS SCRIPTS DEPLOYED SUCCESSFULLY!" -ForegroundColor Green
    } else {
        Write-Host "[WARNING] Some scripts failed to deploy" -ForegroundColor Yellow
    }
    
    Write-Host ""
    Write-Host "DEPLOYED SCRIPTS:" -ForegroundColor Yellow
    foreach ($ScriptInfo in $VpsScripts) {
        $ScriptName = Split-Path $ScriptInfo.Remote -Leaf
        Write-Host "  • $ScriptName - $($ScriptInfo.Description)" -ForegroundColor White
    }
    
    Write-Host ""
    Write-Host "USAGE EXAMPLES:" -ForegroundColor Yellow
    Write-Host "=" * 50 -ForegroundColor Blue
    Write-Host "SSH to server:" -ForegroundColor White
    Write-Host "  ssh -i `"$PrivateKeyPath`" ${ServerUser}@${ServerHost}" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Custom Fields:" -ForegroundColor White
    Write-Host "  python3 $VpsScriptsDir/create_custom_fields_on_vps.py" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Comment Dates:" -ForegroundColor White
    Write-Host "  python3 $VpsScriptsDir/update_comment_dates.py '{\"601\": \"2025-07-08 14:22:00\"}'" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Group Features:" -ForegroundColor White
    Write-Host "  python3 $VpsScriptsDir/update_group_features.py --view-group 38" -ForegroundColor Gray
    Write-Host "  python3 $VpsScriptsDir/update_group_features.py --update-all" -ForegroundColor Gray
    Write-Host ""
    
    Write-Host "IMPORTANT: Update Python code to use new paths!" -ForegroundColor Yellow
    Write-Host "  1. migrators/custom_field_migrator.py" -ForegroundColor White
    Write-Host "  2. config/settings.py" -ForegroundColor White
    Write-Host "  3. migrators/space_migrator.py" -ForegroundColor White
    
} catch {
    Write-Host "CRITICAL ERROR: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
} 