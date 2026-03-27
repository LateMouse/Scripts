$extensions = @('*.iso', '*.zip', '*.rar')

Get-ChildItem -Path '.\*' -File -Include $extensions | ForEach-Object {
    $md5    = (Get-FileHash -Path $_.FullName -Algorithm MD5).Hash
    $sha1   = (Get-FileHash -Path $_.FullName -Algorithm SHA1).Hash
    $sha256 = (Get-FileHash -Path $_.FullName -Algorithm SHA256).Hash

    $content = @"
md5: `t `t$md5
sha1: `t `t$sha1
sha256: `t$sha256
"@

    $hashFile = Join-Path $_.DirectoryName "$($_.BaseName).hash"
    $content | Out-File -FilePath $hashFile -Encoding utf8

    Write-Host "Create $hashFile"
}