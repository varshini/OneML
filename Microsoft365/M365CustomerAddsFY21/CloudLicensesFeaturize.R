library(readxl)
library(dplyr)
library(tidyverse)
library(data.table)
require(TTR)
require(quantmod)
require(caTools)
options(stringsAsFactors = FALSE)

smctpids <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\SMCTPIDs.csv")
colnames(smctpids)[1] <- "FinalTPID"
smctpids <- filter(smctpids, FY21.Subsegment %in% c("SM&C Government - Corporate", "Enterprise Growth", "SM&C Commercial - Corporate") ) #27980 Tpids

#licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\AllLicensesByRevSumCat.csv")
licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\ResCubeSeatsData.csv")
licenses <- inner_join(smctpids, licenses, by = "FinalTPID")
licenses[is.na(licenses)] <- 0

licenses$ym <- format(as.Date(licenses$Date, "%m/%d/%Y"), "%Y%m")
licenses <- licenses %>% select(FinalTPID, ym, Rev_Sum_Category, SeatCount)
licenses_wide <- licenses %>% spread(Rev_Sum_Category, SeatCount)
licenses_wide[is.na(licenses_wide)] <- 0

# Sum of license skus
licenses_wide$M365E3 <- licenses_wide$`O365 - M365 Business` + licenses_wide$`O365 - M365 E3 CAO` + licenses_wide$`O365 - M365 E3 FUSL`
licenses_wide$M365E5 <- licenses_wide$`O365 E5 - M365 E5 CAO` + licenses_wide$`O365 E5 - M365 E5 FUSL`
licenses_wide$TotalM365 <- licenses_wide$M365E3 + licenses_wide$M365E5 + licenses_wide$`O365 - M365 F1`

licenses_wide$EMSE3 <- licenses_wide$`EMS E3 Suite CAO` + licenses_wide$`EMS E3 Suite FUSL`
licenses_wide$EMSE5 <- licenses_wide$`EMS E5 Suite CAO` + licenses_wide$`EMS E5 Suite FUSL` + licenses_wide$`EMS E5 K Suite`
licenses_wide$TotalEMS <- licenses_wide$EMSE3 + licenses_wide$EMSE5
  
licenses_wide$TotalO365 <- licenses_wide$`O365 Plan E1/E2` + licenses_wide$`O365 Plan E1/E2 Cloud Add-On` + 
  licenses_wide$`O365 Plan E3` + licenses_wide$`O365 Plan E3 Cloud Add-On` + licenses_wide$`O365 Plan E4` +
  licenses_wide$`O365 Plan E4 Cloud Add-On` +  licenses_wide$`O365 Plan E5` + licenses_wide$`O365 Plan E5 Cloud Add-On`  

# function to featurize license data

Trending <- function(x){
  n <- length(x)
  x[ x == 0 ] <- 1
  if(n<6 || sum(x>0)<6 || any(is.na(x))) return(rep(0, n))
  cts <- caTools::runmean(x, 3, align = 'right')
  ROC(cts, 1, type = "discrete")
}

#Trends, Mean, Actual Snapshots, Ratios
featurizeSeats <- function(cohort_data, cohort_ym) 
{

  cohort_data <- cohort_data %>% filter(ym <= cohort_ym)
  cohort_data <- cohort_data %>% filter(ym >= "201807" & ym != "0")
  features <- cohort_data %>% 
    group_by(FinalTPID) %>% 
    arrange(ym) %>% 
      mutate(AdvancedComplianceTrend = Trending(`Advanced Compliance`),
             AdvancedSecurityManagementTrend = Trending(`Advanced Security Management`),
             AdvancedThreatProtectionPlan1Trend = Trending(`Advanced Threat Protection Plan 1`),
             AdvancedThreatProtectionPlan2Trend = Trending(`Advanced Threat Protection Plan 2`),
             AzureActiveDirectoryTrend = Trending(`Azure Active Directory`),
             AzureActiveDirectoryPremP2Trend = Trending(`Azure Active Directory Prem P2`),
             AzureATPTrend = Trending(`Azure ATP`),
             AzureInfoProtPremP2Trend = Trending(`Azure Info Prot Prem P2`),
            AzureMobIdentitySvcsTrend = Trending(`Azure Mob & Identity Svcs`),
            AzureRemoteAppTrend = Trending(`Azure RemoteApp`),
            AzureRightsManagementServicesTrend = Trending(`Azure Rights Management Services`),
            CloudAppSecurityTrend = Trending(`Cloud App Security`),
            EMSM365BusinessTrend = Trending(`EMS - M365 Business`),
            EMSM365E3CAOTrend = Trending(`EMS - M365 E3 CAO`),
            #EMSM365E3EDUTrend = Trending(`EMS - M365 E3 EDU`),
            EMSM365E3FUSLTrend = Trending(`EMS - M365 E3 FUSL`),
            EMSM365F1Trend = Trending(`EMS - M365 F1`),
            EMSE3SuiteCAOTrend = Trending(`EMS E3 Suite CAO`),
            EMSE3SuiteFUSLTrend = Trending(`EMS E3 Suite FUSL`),
            EMSE5KSuiteTrend = Trending(`EMS E5 K Suite`),
            EMSE5SuiteCAOTrend = Trending(`EMS E5 Suite CAO`),
            EMSE5SuiteFUSLTrend = Trending(`EMS E5 Suite FUSL`),
            IntuneTrend = Trending(`Intune`),
            IntuneClientTrend = Trending(`Intune - Client`),
            MyAnalyticsTrend = Trending(`MyAnalytics`),
            #O365M365A3EDUTrend = Trending(`O365 - M365 A3 EDU`),
            O365M365BusinessTrend = Trending(`O365 - M365 Business`),
            O365M365E3CAOTrend = Trending(`O365 - M365 E3 CAO`),
            O365M365E3FUSLTrend = Trending(`O365 - M365 E3 FUSL`),
            #O365M365F0Trend = Trending(`O365 - M365 F0`),
            O365M365F1Trend = Trending(`O365 - M365 F1`),
            #O365E5M365A5EDUTrend = Trending(`O365 E5 - M365 A5 EDU`),
            O365E5M365E5CAOTrend = Trending(`O365 E5 - M365 E5 CAO`),
            O365E5M365E5FUSLTrend = Trending(`O365 E5 - M365 E5 FUSL`),
            O365PlanE1E2Trend = Trending(`O365 Plan E1/E2`),
            O365PlanE1E2CloudAddOnTrend = Trending(`O365 Plan E1/E2 Cloud Add-On`),
            O365PlanE3Trend = Trending(`O365 Plan E3`),
            O365PlanE3CloudAddOnTrend = Trending(`O365 Plan E3 Cloud Add-On`),
            O365PlanE4Trend = Trending(`O365 Plan E4`),
            O365PlanE4CloudAddOnTrend = Trending(`O365 Plan E4 Cloud Add-On`),
            O365PlanE5Trend = Trending(`O365 Plan E5`),
            O365PlanE5CloudAddOnTrend = Trending(`O365 Plan E5 Cloud Add-On`),
            PowerBIOfficeSuitesTrend = Trending(`Power BI - Office Suites`),
            PowerBIStandaloneProTrend = Trending(`Power BI - Standalone Pro`),
            PowerBIPremiumTrend = Trending(`Power BI Premium`),
            PowerBISuitesM365Trend = Trending(`Power BI Suites - M365`),
            M365E3Trend = Trending(`M365E3`),
            M365E5Trend = Trending(`M365E5`),
            M365F1Trend = Trending(`O365 - M365 F1`),
            EMSE3Trend = Trending(`EMSE3`),
            EMSE5Trend = Trending(`EMSE5`), 
            AdvancedComplianceMean = mean(`Advanced Compliance`),
            AdvancedSecurityManagementMean = mean(`Advanced Security Management`),
            AdvancedThreatProtectionPlan1Mean = mean(`Advanced Threat Protection Plan 1`),
            AdvancedThreatProtectionPlan2Mean = mean(`Advanced Threat Protection Plan 2`),
            AzureActiveDirectoryMean = mean(`Azure Active Directory`),
            AzureActiveDirectoryPremP2Mean = mean(`Azure Active Directory Prem P2`),
            AzureATPMean = mean(`Azure ATP`),
            AzureInfoProtPremP2Mean = mean(`Azure Info Prot Prem P2`),
            AzureMobIdentitySvcsMean = mean(`Azure Mob & Identity Svcs`),
            AzureRemoteAppMean = mean(`Azure RemoteApp`),
            AzureRightsManagementServicesMean = mean(`Azure Rights Management Services`),
            CloudAppSecurityMean = mean(`Cloud App Security`),
            EMSM365BusinessMean = mean(`EMS - M365 Business`),
            EMSM365E3CAOMean = mean(`EMS - M365 E3 CAO`),
            #EMSM365E3EDUMean = mean(`EMS - M365 E3 EDU`),
            EMSM365E3FUSLMean = mean(`EMS - M365 E3 FUSL`),
            EMSM365F1Mean = mean(`EMS - M365 F1`),
            EMSE3SuiteCAOMean = mean(`EMS E3 Suite CAO`),
            EMSE3SuiteFUSLMean = mean(`EMS E3 Suite FUSL`),
            EMSE5KSuiteMean = mean(`EMS E5 K Suite`),
            EMSE5SuiteCAOMean = mean(`EMS E5 Suite CAO`),
            EMSE5SuiteFUSLMean = mean(`EMS E5 Suite FUSL`),
            IntuneMean = mean(`Intune`),
            IntuneClientMean = mean(`Intune - Client`),
            MyAnalyticsMean = mean(`MyAnalytics`),
            #O365M365A3EDUMean = mean(`O365 - M365 A3 EDU`),
            O365M365BusinessMean = mean(`O365 - M365 Business`),
            O365M365E3CAOMean = mean(`O365 - M365 E3 CAO`),
            O365M365E3FUSLMean = mean(`O365 - M365 E3 FUSL`),
            #O365M365F0Mean = mean(`O365 - M365 F0`),
            O365M365F1Mean = mean(`O365 - M365 F1`),
            #O365E5M365A5EDUMean = mean(`O365 E5 - M365 A5 EDU`),
            O365E5M365E5CAOMean = mean(`O365 E5 - M365 E5 CAO`),
            O365E5M365E5FUSLMean = mean(`O365 E5 - M365 E5 FUSL`),
            O365PlanE1E2Mean = mean(`O365 Plan E1/E2`),
            O365PlanE1E2CloudAddOnMean = mean(`O365 Plan E1/E2 Cloud Add-On`),
            O365PlanE3Mean = mean(`O365 Plan E3`),
            O365PlanE3CloudAddOnMean = mean(`O365 Plan E3 Cloud Add-On`),
            O365PlanE4Mean = mean(`O365 Plan E4`),
            O365PlanE4CloudAddOnMean = mean(`O365 Plan E4 Cloud Add-On`),
            O365PlanE5Mean = mean(`O365 Plan E5`),
            O365PlanE5CloudAddOnMean = mean(`O365 Plan E5 Cloud Add-On`),
            PowerBIOfficeSuitesMean = mean(`Power BI - Office Suites`),
            PowerBIStandaloneProMean = mean(`Power BI - Standalone Pro`),
            PowerBIPremiumMean = mean(`Power BI Premium`),
            PowerBISuitesM365Mean = mean(`Power BI Suites - M365`),
            M365E3Mean = mean(`M365E3`),
            M365E5Mean = mean(`M365E5`),
            M365F1Mean = Trending(`O365 - M365 F1`),
            EMSE3Mean = mean(`EMSE3`),
            EMSE5Mean = mean(`EMSE5`))
  features <- features %>% filter(ym == cohort_ym)
  features <- features %>% rename(AdvancedCompliance = `Advanced Compliance`,
                                   AdvancedSecurityManagement = `Advanced Security Management`,
                                   AdvancedThreatProtectionPlan1 = `Advanced Threat Protection Plan 1`,
                                   AdvancedThreatProtectionPlan2 = `Advanced Threat Protection Plan 2`,
                                   AzureActiveDirectory = `Azure Active Directory`,
                                   AzureActiveDirectoryPremP2 = `Azure Active Directory Prem P2`,
                                   AzureATP = `Azure ATP`,
                                   AzureInfoProtPremP2 = `Azure Info Prot Prem P2`,
                                   AzureMobIdentitySvcs = `Azure Mob & Identity Svcs`,
                                   AzureRemoteApp = `Azure RemoteApp`,
                                   AzureRightsManagementServices = `Azure Rights Management Services`,
                                   CloudAppSecurity = `Cloud App Security`,
                                   EMSM365Business = `EMS - M365 Business`,
                                   EMSM365E3CAO = `EMS - M365 E3 CAO`,
                                   #EMSM365E3EDU = `EMS - M365 E3 EDU`,
                                   EMSM365E3FUSL = `EMS - M365 E3 FUSL`,
                                   EMSM365F1 = `EMS - M365 F1`,
                                   EMSE3SuiteCAO = `EMS E3 Suite CAO`,
                                   EMSE3SuiteFUSL = `EMS E3 Suite FUSL`,
                                   EMSE5KSuite = `EMS E5 K Suite`,
                                   EMSE5SuiteCAO = `EMS E5 Suite CAO`,
                                   EMSE5SuiteFUSL = `EMS E5 Suite FUSL`,
                                   Intune = `Intune`,
                                   IntuneClient = `Intune - Client`,
                                   MyAnalytics = `MyAnalytics`,
                                   #O365M365A3EDU = `O365 - M365 A3 EDU`,
                                   O365M365Business = `O365 - M365 Business`,
                                   O365M365E3CAO = `O365 - M365 E3 CAO`,
                                   O365M365E3FUSL = `O365 - M365 E3 FUSL`,
                                   #O365M365F0 = `O365 - M365 F0`,
                                   O365M365F1 = `O365 - M365 F1`,
                                   #O365E5M365A5EDU = `O365 E5 - M365 A5 EDU`,
                                   O365E5M365E5CAO = `O365 E5 - M365 E5 CAO`,
                                   O365E5M365E5FUSL = `O365 E5 - M365 E5 FUSL`,
                                   O365PlanE1E2 = `O365 Plan E1/E2`,
                                   O365PlanE1E2CloudAddOn = `O365 Plan E1/E2 Cloud Add-On`,
                                   O365PlanE3 = `O365 Plan E3`,
                                   O365PlanE3CloudAddOn = `O365 Plan E3 Cloud Add-On`,
                                   O365PlanE4 = `O365 Plan E4`,
                                   O365PlanE4CloudAddOn = `O365 Plan E4 Cloud Add-On`,
                                   O365PlanE5 = `O365 Plan E5`,
                                   O365PlanE5CloudAddOn = `O365 Plan E5 Cloud Add-On`,
                                   PowerBIOfficeSuites = `Power BI - Office Suites`,
                                   PowerBIStandalonePro = `Power BI - Standalone Pro`,
                                   PowerBIPremium = `Power BI Premium`,
                                   PowerBISuitesM365 = `Power BI Suites - M365`,
                                   M365E3 = `M365E3`,
                                   M365E5 = `M365E5`,
                                   M365F1 = `O365 - M365 F1`,
                                   EMSE3 = `EMSE3`,
                                   EMSE5 = `EMSE5`) 
  
  #Per workload/Total
  features$o365E3Perc <- (features$O365PlanE3 + features$O365PlanE3CloudAddOn)/features$TotalO365
  features$o365e5Perc <- (features$O365PlanE5 + features$O365PlanE5CloudAddOn)/features$TotalO365
  features$o365E12Perc <- (features$O365PlanE1E2 + features$O365PlanE1E2CloudAddOn)/features$TotalO365
  features$m365e3Perc <- features$M365E3/features$TotalM365
  features$m365e5perc <- features$M365E5/features$TotalM365
  features$m365f1perc <- features$M365F1/features$TotalM365
  features$emse3perc <- features$EMSE3/features$TotalEMS
  features$emse5perc <- features$EMSE5/features$TotalEMS
  features$O365E3TotalE3 <- features$O365PlanE3/(features$O365PlanE3 + features$EMSE3 + features$M365E3)
  features$EMSE3TotalE3 <- features$EMSE3/(features$O365PlanE3 + features$EMSE3 + features$M365E3)
  features$O365E5TotalE5 <- features$O365PlanE5/(features$O365PlanE5 + features$EMSE5 + features$M365E5)
  features$EMSE5TotalE5 <- features$EMSE5/(features$O365PlanE5 + features$EMSE5 + features$M365E5)
  
  return (features)
}

####### TRAINING PERIOD #########

seats_training <- featurizeSeats(licenses_wide, "201912") #Trends, Mean, Actual Snapshots, Ratios
seats_training[is.na(seats_training)] <- 0 
seats_training <- rapply(seats_training, f=function(x) ifelse(is.nan(x),0,x), how="replace" )
seats_training <- rapply(seats_training, f=function(x) ifelse(is.infinite(x),0,x), how="replace" )
write.csv(seats_training, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Seats\\Training_Seats.csv", row.names = FALSE)

####### SCORING PERIOD #########

seats_scoring <- featurizeSeats(licenses_wide, "202006") #Trends, Mean, Actual Snapshots, Ratios
seats_scoring[is.na(seats_scoring)] <- 0 
seats_scoring <- rapply(seats_scoring, f=function(x) ifelse(is.nan(x),0,x), how="replace" )
seats_scoring <- rapply(seats_scoring, f=function(x) ifelse(is.infinite(x),0,x), how="replace" )
write.csv(seats_scoring, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Seats\\Scoring_Seats.csv", row.names = FALSE)


