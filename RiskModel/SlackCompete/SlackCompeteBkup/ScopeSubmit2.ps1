<#
.SYNOPSIS
Submits a script to process an input streamset accross a range of dates.

.DESCRIPTION
Given a SCOPE script file on your local machine, formatted with a pre-processing string parameter @@PROCESS_DATE@@, this cmdlet will automatically submit a Cosmos job with your script, with each date value between the start and end dates automatically substituted in for each submission. This allows you to limit the processing size of each individual job and cleanly output a single file per day across a range of days. This cdmlet assumes you have Cosmos Powershell installed and have set valid credentials using that same module.

.LINK
https://microsoft.sharepoint.com/teams/Cosmos/CosmosPowerShell/Home.aspx

.PARAMETER ScriptPath
Path of the script on the local machine to be submitted

.PARAMETER StartDate

.PARAMETER EndDate

.PARAMETER VC
Specifies the VC to submit the job to. Default is "vc://cosmos14/ACE.proc". Other VCs on COSMOS14 also do not need to include the leading cluster information (ie omit "vc://cosmos14/")
For VCs on other clusters, specify full VC path

.PARAMETER Unit
(Optional) Specify the period interval unit, valid values are: "Day", "Month", "Year". Default value is Day

.PARAMETER Period
(Optional) Specify the period interval length, default value is 1

.PARAMETER ConcurrentJobs
(Optional) How many jobs should be submitted to the VC at a time. default value is 10

.PARAMETER WaitTime
(Optional) Specify number of seconds to wait before trying again after job limit hit.

.PARAMETER Priority
(Optional) set job priority (default 1100)

.PARAMETER ValidateStreamSetPath
(Optional) Relative path of the (generalized) streamset on the VC to validate before submitting the job. Substitute flags for actual date time values,
ie "/users/TwcFileIO/CobaltReports/Prod%24/SummaryLogs/SummaryLogs_{0:yyyy-MM-dd}.ss"

.PARAMETER ScopeParams
(Optional) Key-value pairs of parameters to be passed to the scope script. Do not overwrite the parameter @@PROCESS_DATE@@

.PARAMETER ScopeParamsJson
(Optional) path of JSON file that contains key-value pairs of parameters and their values to pass to the scope script. Do not overwrite the parameter @@PROCESS_DATE@@

.PARAMETER NoOp
(Optional) If $TRUE, it will only 'pretend' to submit the job. Used for testing scripts without actually submitting jobs.

.RETURNVALUE
Array of Cosmos job objects

.EXAMPLE
Create time series C2R, hourly basis:
Submit-ScopeScript -ScriptPath ..\..\anomalydetection\ClickToRun\C2RFeatures.script -StartDate "2014-01-01" -EndDate "2014-10-13" -VC OSIAdhoc -Unit Hour -ScopeParams @{ADUtilities_dll="C:\o\r\n\dev\datascience\anomalydetection\ADUtilities\bin\Debug\Microsoft.Office.DataScience.ADUtilities.dll"}

.EXAMPLE
Summarizes hourly basis C2R time series stream into daily basis output:
Submit-ScopeScript -ScriptPath ..\..\anomalydetection\ClickToRun\C2RToDQLog.script -StartDate "2014-01-01" -EndDate "2014-01-02" -VC OSIAdhoc -Unit Day -ScopeParams @{ADUtilities_dll="C:\o\r\n\dev\datascience\anomalydetection\ADUtilities\bin\Debug\Microsoft.Office.DataScience.ADUtilities.dll"}

.EXAMPLE
SQM daily basis pipeline:
Submit-ScopeScript -ScriptPath ..\..\anomalydetection\SQM\SQMFeatures.script -StartDate "2014-01-01" -EndDate "2014-12-31" -VC OSIAdhoc -Unit Day -ScopeParams @{ADUtilities_dll="C:\o\r\n\dev\datascience\anomalydetection\ADUtilities\bin\Debug\Microsoft.Office.DataScience.ADUtilities.dll"}
Submit-ScopeScript -ScriptPath ..\..\anomalydetection\SQM\SQMToDQLog.script -StartDate "2014-01-01" -EndDate "2014-12-31" -VC OSIAdhoc -Unit Day -ScopeParams @{ADUtilities_dll="C:\o\r\n\dev\datascience\anomalydetection\ADUtilities\bin\Debug\Microsoft.Office.DataScience.ADUtilities.dll"}

#>

function Get-Date-Intervals
{
    [CmdletBinding()]
    param(
        [Parameter(Position=0, Mandatory=$true)]
        [DateTime]$StartDate,

        [Parameter(Position=1, Mandatory=$true)]
        [DateTime]$EndDate,

        [Parameter(Position=2, Mandatory=$true)]
        [ValidateSet('Year','Month','Day','Hour')]
        [string]$Unit,

        [Parameter(Position=3, Mandatory=$true)]
        [int]$Period
    )

    $NextEnd = $StartDate
    $NextStart = $StartDate

    $intervals = @()

    while($NextEnd -le $EndDate)
    {
        switch($Unit)
        {
            "Year"  {$NextEnd = $NextStart.AddYears($Period)}
            "Month" {$NextEnd = $NextStart.AddMonths($Period)}
            "Day"   {$NextEnd = $NextStart.AddDays($Period)}
            "Hour"  {$NextEnd = $NextStart.AddHours($Period)}
        }
        
        # we want to provide a closed interval so it can be used with '...' operator when defining stream set range in Scope
        $NextEnd = $NextEnd.AddMilliseconds(-1)
        
        $EndDatePass = $NextEnd
        if($EndDatePass -gt $EndDate) 
        {
            $EndDatePass = $EndDate
        }

        $intervals += @{"Start" = $NextStart; "End"= $EndDatePass}

        $NextStart = $NextEnd.AddMilliseconds(1)
    }

    return $intervals
}

function Submit-ScopeScript
{
    [CmdletBinding()]
    param(
        [Parameter(Position=0, Mandatory=$true)]
        [string]$ScriptPath,

        [Parameter(Position=1, Mandatory=$true)]
        [ValidatePattern("\d{4}-\d{1,2}-\d{1,2}")]
        [string]$StartDate,

        [Parameter(Position=2, Mandatory=$true)]
        [ValidatePattern("\d{4}-\d{1,2}-\d{1,2}")]
        [string]$EndDate,

        [Parameter(Position=3, Mandatory=$true)]
        [string]$VC='ACE.proc',

        [Parameter(Position=4)]
        [ValidateSet('Year','Month','Day','Hour')]
        [string]$Unit='Day',

        [Parameter(Position=5)]
        [int]$Period=1,

	    [Parameter(Position=6)]
	    [int]$ConcurrentJobs=10,

	    [Parameter(Position=7)]
	    [int]$WaitTime=30,

        [Parameter(Position=8)]
	    [int]$Priority=1100,

        [Parameter(Position=9)]
        [bool]$NoOp=$FALSE,

        [string]$ValidateStreamSetPath='',

        [hashtable]$ScopeParams = @{},

        [string]$ScopeParamsJson=''
    )

    if($NoOp -eq $TRUE)
    {
        Write-Host "NoOp flag enabled. Will only 'pretend' to submit jobs. No operations will be performed." -ForegroundColor Yellow
    }

    if($VC.StartsWith('vc://'))
    {
        $vcName = $VC
    }
    else
    {
        $vcName = 'vc://cosmos14/{0}' -f $VC
    }

    if(($Priority -le 1000) -and ($ConcurrentJobs -gt 2) -and ($vcName.Contains('asimov')) )
    {
        $ConcurrentJobs = 2
        Write-Host "Number of concurrent jobs reset to $ConcurrentJobs, because Priority is set lower than 1000." -ForegroundColor Yellow
    }

    $jobArray = @()

    $scriptName = (Get-Item -literalPath $ScriptPath).Name.Replace('.script', '')

    $startDateTime = [System.DateTime]::Parse($StartDate)
    $endDateTime = [System.DateTime]::Parse($EndDate)

    $intervals = Get-Date-Intervals -StartDate $startDateTime -EndDate $endDateTime -Unit $Unit -Period $Period
    
    if($intervals.Length -eq 0)
    {
        throw 'StartTime must be before EndTime'
    }

    $intervals | Foreach-Object {
        $day = $_.Start
        $dayEnd = $_.End

        # currently Scope only supports date (no datetime) for defining streamset names.
        # but there is hour parameter for working around
        $dayStr = $day.ToString('yyyy-MM-dd')
        $dayEndStr = $dayEnd.ToString('yyyy-MM-dd')
        $hour = $day.ToString('HH')

        $jobName = "$scriptName-$dayStr-$hour"

        $params = @{ 'PROCESS_DATE' = $dayStr; 'PROCESS_DATE_START' = $dayStr; 'PROCESS_DATE_END' = $dayEndStr; 'PROCESS_DATE_HOUR' = $hour}

        $validateStreamPath = [string]::Format(("$vcName$ValidateStreamSetPath"), $day)
        if (!$ValidateStreamSetPath -or (Test-CosmosStream $validateStreamPath))
        {
		#Was Get-CosmosCredential
            $currentUser = $env:USERDOMAIN + "\" + $env:UserName
            $attemptsCounter = 0
            do
            {
                try
                {
                    if ($vcName.Contains('asimov.partner.osg'))
                    {
                        $qCount = (Get-CosmosJob $vcName -User $currentUser | Where-Object { $_.State -eq "Queued" } | Measure-Object -Line).Lines
                        $ConcurrentqJobs = 1
                        $ConcurrentJobs = 2
                    } 
                    else
                    {
                        $qCount = 0
                        $ConcurrentqJobs = 1
                    }

                    $jobCount = (Get-CosmosJob $vcName -User $currentUser | Where-Object { $_.State -eq "Running" -or $_.State -eq "Queued" } | Measure-Object -Line).Lines
                }
                catch [VcClientException]
                {
                    Write-Verbose 'There appears to be a delay in reaching the front-end service. Sleeping for 30 seconds then retrying.'
                    $jobCount = $ConcurrentJobs 
                }
                catch [AuthenticationException]
                {
                    throw "It appears Cosmos isn't accepting your credentials. Please correct them using the Set-CosmosCredential cmdlet and retry"
                }
                if (($jobCount -lt $ConcurrentJobs) -and ($qCount -lt $ConcurrentqJobs))
                {
                    if ($ScopeParamsJson)
                    {
                        $json = (Get-Content $ScopeParamsJson) -join "`n" | ConvertFrom-Json
                        foreach ($key in $json.psobject.properties.name)
                        {
                            $params.Add($key, $json.$key)
                        }
                    }
                    elseif ($ScopeParams)
                    {
                        foreach ($key in $ScopeParams.keys)
                        {
                            $params.Add($key, $ScopeParams[$key])
                        }
                    }
			        # Removed -FriendlyName $jobName
                    #adding a do while loop to retry submissions on random exceptions
                    $jobsubmitattemps = 0
                    $jobNotSubmitted = 1 
                    do
                    {
                        try
                        {
                            $jobsubmitattemps ++
                            if($NoOp -eq $FALSE)
                            {
                                $job = Submit-CosmosScopeJob -ScriptPath $ScriptPath -Parameters $params -VC $vcName -Priority $Priority
                                $jobArray += $job
                            }
                            $jobNotSubmitted = 0
                        }
                        catch
                        {
                            Write-Host "A $($_.Exception.GetType().FullName) exception was encountered submitting the job for $dayStr. Trying again" -ForegroundColor Yellow
                            Write-Host "Exception: $($_.Exception.Message)" -ForegroundColor Red

                        }

                    } while ($jobNotSubmitted -eq 1 -and $jobsubmitattemps -le 5)

                    if($jobNotSubmitted -eq 0)
                    {


                        if($NoOp -eq $TRUE)
                        {
                            $jobState = "Running"
                        }
                        else
                        {
                            Start-Sleep 10
                            $jobState = (Get-CosmosJob $vcName -Name $job.Name).State
                        }
                        
                        if($jobState -eq 'Running' -or $jobState -eq 'Queued' -or $NoOp -eq $TRUE)
                        {
                            Write-Host ([string]::Format('Successfully submitted {0} for {1} to VC {2}. There are now {3} of your jobs running on the VC.', $job.Name, $dayStr, $VC, $($jobCount + 1))) -foregroundcolor green
                        }
                        break
                    }
                    else
                    {
                        Write-Host "Could not submit the job for $dayStr. Try submitting manually." -ForegroundColor Red
                        break
                    }
                }
                else
                {
                    Write-Host ([string]::Format("Since you currently have {0} jobs running, we won't submit {1} for {2} yet. It's been {3} minutes since we last submitted, sleeping for {4} seconds.", $jobCount, $job.Name, $dayStr, $($attemptsCounter * ($WaitTime/60.0)), $WaitTime)) -foregroundcolor yellow
                    Start-Sleep $WaitTime
                }
                $attemptsCounter++
            }
            while($attemptsCounter -le 600)
        }
        else
        {
            Write-Warning ([string]::Format('Warning, stream {0} cannot be found, moving onto the next stream to attempt', $validateStreamPath))
        }
    }

    return $jobArray
}


<#
.SYNOPSIS
Waits for each Cosmos job to complete before continuing execution. Will wait up to 2 hours only.

.DESCRIPTION
Given an Array of Cosmos Job objects, calls Wait-CosmosJob on each one until all jobs are complete. Complete could include 'failed'.

.LINK
https://microsoft.sharepoint.com/teams/Cosmos/CosmosPowerShell/Home.aspx

.PARAMETER JobList
An array of Cosmos Job objects.

.RETURNVALUE
None

.EXAMPLE
Submit a batch of jobs and get the job list.  Then wait for all to complete.
$userStreams = Submit-ScopeScript -Script "2  - Daily Mobile Active Users.script" -StartDate $startDate -EndDate $endDate -VC $vcOfficeAdhoc -Unit Day -Period 1 -ConcurrentJobs $concurrentJobs -WaitTime 60 -Priority $priority -NoOp $TestOnly
WaitForAllJobs($userStreams)
#>
function WaitForAllJobs()
{
    [CmdletBinding()]
    param(
        [Parameter(Position=0, Mandatory=$true)]
        [array]$JobList
    )

    if($JobList.Length -eq 0)
    {
        Write-Host "WaitForAllJobs:  JobList is empty." -ForegroundColor Yellow   
    }
    foreach($job in $JobList)
    {
        if($job -ne $null)
        {
            $jobName = $job.Name
            Write-Host "Will wait for job: $jobName"
            Wait-CosmosJob $job -TimeOut 7200 #Wait two hours max
        }
    }
}