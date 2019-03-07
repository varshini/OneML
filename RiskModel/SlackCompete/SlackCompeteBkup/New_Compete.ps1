# powershell jobs script

# Source file:   \OfficeDataScience\Projects\Compete\Compete\Generic-Automation.ps1
 
#Setup
$UsefulScriptsFile = "C:\Users\varamase\source\repos\SMC_NCA\ScopeSubmit2.ps1"  #$PSScriptRoot
if(-not (Test-path $UsefulScriptsFile))
{
    Throw "Can't find ScopeSubmit2.ps1 at path: $UsefulScriptsFile"
}
#Load functions
. $UsefulScriptsFile
 
 
#Location of Scope Script

#$ScriptRootPath = "C:\Users\frhokhol\source\repos\SlackTeamsAnyTimeIntervalCensus\SlackTeamsAnyTimeIntervalCensus\Scope.script"  #$PSScriptRoot

$ScriptRootPath = "C:\Users\varamase\source\repos\SlackCompete\SlackCompete\New_Modified_Compete.script"  #$PSScriptRoot
 
#External Date Parameters passed to scope script are:
#    @@PROCESS_DATE@@ == @@PROCESS_DATE_START@@
#    @@PROCESS_DATE_END@@
#    @@PROCESS_DATE_HOUR@@ #if using hourly
 
$userStreams = Submit-ScopeScript -Script $ScriptRootPath `
                                  -ScopeParams @{} `
                                  -StartDate "2017-09-01" `
                                  -EndDate "2017-09-30" `
                                  -VC "vc://cosmos14/ACE.proc" `
                                  -Unit Day `
                                  -Period 1 `
                                  -ConcurrentJobs 30 `
                                  -WaitTime 60 `
                                  -Priority 1100 `
                                  -NoOp $FALSE 
WaitForAllJobs($userStreams)
