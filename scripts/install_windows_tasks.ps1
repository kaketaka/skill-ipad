param(
  [string]$TaskPrefix = "MarketSimTrader"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Runner = Join-Path $Root "scripts\run_daily.ps1"

$jpAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$Runner`" -Mode JP"
$jpTrigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 16:10
Register-ScheduledTask -TaskName "$TaskPrefix-JP-AfterClose" -Action $jpAction -Trigger $jpTrigger -Description "Run JP paper trading after Tokyo close." -Force

$usAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$Runner`" -Mode US"
$usTrigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Tuesday,Wednesday,Thursday,Friday,Saturday -At 07:15
Register-ScheduledTask -TaskName "$TaskPrefix-US-AfterClose" -Action $usAction -Trigger $usTrigger -Description "Run US paper trading after New York close, in Japan time." -Force

$reviewAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$Runner`" -Mode REVIEW"
$reviewTrigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday,Saturday -At 22:30
Register-ScheduledTask -TaskName "$TaskPrefix-NightlyReview" -Action $reviewAction -Trigger $reviewTrigger -Description "Create nightly paper-trading review." -Force

Write-Host "Installed scheduled tasks with prefix $TaskPrefix"
