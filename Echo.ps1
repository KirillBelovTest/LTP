while ($true) {
    wolframscript -f Echo.wls

    "Exit code == $LASTEXITCODE"
    
    Start-Sleep -Seconds 1
}