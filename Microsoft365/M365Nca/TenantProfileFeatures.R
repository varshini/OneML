library(CosmosToR)
library(dplyr)
options(stringsAsFactors = FALSE)



featurize <- function(cohort_data, cohort_end_date, ym)
{
  cohort_data$FirstPaidStartDate <- as.Date(cohort_data$FirstPaidStartDate, "%m/%d/%Y %H:%M:%S")
  cohort_data$Tenure <- elapsed_months(as.Date(cohort_end_date), cohort_data$FirstPaidStartDate)
  cohort_data$Tenure[is.na(cohort_data$Tenure)] <- 0
  
  cohort_data$HasTrial <- as.integer(as.logical(cohort_data$HasTrial))
  cohort_data$IsM365 <- as.integer(as.logical(cohort_data$IsM365))
  cohort_data$HasM365PaidSeats <- as.integer(as.logical(cohort_data$HasM365PaidSeats))
  cohort_data$HasM365SKUBusiness <- as.integer(as.logical(cohort_data$HasM365SKUBusiness))
  cohort_data$HasM365SKUF1 <- as.integer(as.logical(cohort_data$HasM365SKUF1))
  cohort_data$HasM365SKUE3 <- as.integer(as.logical(cohort_data$HasM365SKUE3))
  cohort_data$HasM365SKUE5 <- as.integer(as.logical(cohort_data$HasM365SKUE5))
  cohort_data$HasEMSSku <- as.integer(as.logical(cohort_data$HasEMSSku))
  cohort_data$HasM365Sku <- as.integer(as.logical(cohort_data$HasM365Sku))
  cohort_data$HasOffice365Sku <- as.integer(as.logical(cohort_data$HasOffice365Sku))
  cohort_data$HasWindowsSku <- as.integer(as.logical(cohort_data$HasWindowsSku))
  cohort_data$HasDynamicsSku <- as.integer(as.logical(cohort_data$HasDynamicsSku))
  cohort_data$HasOfficeSKUE1 <- as.integer(as.logical(cohort_data$HasOfficeSKUE1))
  cohort_data$HasOfficeSKUE3 <- as.integer(as.logical(cohort_data$HasOfficeSKUE3))
  cohort_data$HasOfficeSKUE4 <- as.integer(as.logical(cohort_data$HasOfficeSKUE4))
  cohort_data$HasOfficeSKUE5 <- as.integer(as.logical(cohort_data$HasOfficeSKUE5))

  # cohort_data <- cohort_data %>% select(FinalTPID, IsFastTrackTenant, Tenure,
  #                        EXOEnabledUsers, SPOEnabledUsers, SFBEnabledUsers, PPDEnabledUsers, TeamsFreemiumEnabledUsers,YammerEnabledUsers, 
  #                        AADPEnabledUsers,AIPEnabledUsers,AATPEnabledUsers,IntuneEnabledUsers,MCASEnabledUsers,WDATPEnabledUsers,
  #                        AudioConferenceEnabledUsers,PhoneSystemEnabledUsers,ComplianceEnabledUsers,OATPEnabledUsers,AADPP2EnabledUsers,
  #                        AIPP2EnabledUsers,WindowsEnabledUsers,TotalUsers,
  #                        O365EnabledUsers,EMSEnabledUsers,M365EnabledUsers,O365E5EnabledUsers,EMSE5EnabledUsers,M365E5EnabledUsers,
  #                        HasTrial,IsM365,HasM365PaidSeats,HasM365SKUBusiness,HasM365SKUF1,HasM365SKUE3,HasM365SKUE5,HasEMSSku,HasM365Sku,
  #                        HasOffice365Sku,HasWindowsSku,HasDynamicsSku,HasOfficeSKUE1,HasOfficeSKUE3,HasOfficeSKUE4,HasOfficeSKUE5, 
  #                        EMSPaidAvailableUnits, OfficePaidAvailableUnits, M365PaidAvailableUnits, 
  #                        EXOPaidAvailableUnits, SPOPaidAvailableUnits, OD4BPaidAvailableUnits, SfbPaidAvailableUnits,
  #                        YammerPaidAvailableUnits, TeamsPaidAvailableUnits, ProPlusPaidAvailableUnits, AADPPaidAvailableUnits, 
  #                        AIPPaidAvailableUnits, AATPPaidAvailableUnits, IntunePaidAvailableUnits, MCASPaidAvailableUnits, 
  #                        WDATPPaidAvailableUnits,AudioConferencePaidAvailableUnits, CompliancePaidAvailableUnits)
  
  
  cohort_data$EXOEnabledPerc <- cohort_data$EXOEnabledUsers/cohort_data$EXOPaidAvailableUnits
  cohort_data$SPOEnabledPerc <- cohort_data$SPOEnabledUsers/cohort_data$SPOPaidAvailableUnits
  cohort_data$SFBEnabledPerc <- cohort_data$SFBEnabledUsers/cohort_data$SfbPaidAvailableUnits
  cohort_data$ProPlusEnabledPerc <- cohort_data$PPDEnabledUsers/cohort_data$ProPlusPaidAvailableUnits
  cohort_data$TeamsEnabledPerc <- cohort_data$TeamsFreemiumEnabledUsers/cohort_data$TeamsFreemiumAvailableUnits
  cohort_data$YammerEnabledPerc <- cohort_data$YammerEnabledUsers/cohort_data$YammerPaidAvailableUnits
  cohort_data$AADPEnabledPerc <- cohort_data$AADPEnabledUsers/cohort_data$AADPPaidAvailableUnits
  cohort_data$AIPEnabledPerc <- cohort_data$AIPEnabledUsers/cohort_data$AIPPaidAvailableUnits
  cohort_data$AATPEnabledPerc <- cohort_data$AATPEnabledUsers/cohort_data$AATPPaidAvailableUnits
  cohort_data$IntuneEnabledPerc <- cohort_data$IntuneEnabledUsers/cohort_data$IntunePaidAvailableUnits
  cohort_data$MCASEnabledPerc <- cohort_data$MCASEnabledUsers/cohort_data$MCASPaidAvailableUnits
  cohort_data$WDATPEnabledPerc <- cohort_data$WDATPEnabledUsers/cohort_data$WDATPPaidAvailableUnits
  cohort_data$AudioConfEnabledPerc <- cohort_data$AudioConferenceEnabledUsers/cohort_data$AudioConferencePaidAvailableUnits
  cohort_data$ThreatIntlEnabledPerc <- cohort_data$ThreatIntelligenceEnabledUsers/cohort_data$ThreatIntelligencePaidAvailableUnits
  cohort_data$PhoneSystemEnabledPerc <- cohort_data$PhoneSystemEnabledUsers/cohort_data$EXOPaidAvailableUnits
  cohort_data$ComplianceEnabledPerc <- cohort_data$ComplianceEnabledUsers/cohort_data$CompliancePaidAvailableUnits
  cohort_data$OATPEnabledPerc <- cohort_data$OATPEnabledUsers/cohort_data$OATPPaidAvailableUnits
  cohort_data$AADPP2EnabledPerc <- cohort_data$AADPP2EnabledUsers/cohort_data$AADPP2PaidAvailableUnits
  cohort_data$AIPP2EnabledPerc <- cohort_data$AIPP2EnabledUsers/cohort_data$AIPP2PaidAvailableUnits
  cohort_data$WindowsEnabledPerc <- cohort_data$WindowsEnabledUsers/cohort_data$WindowsPaidAvailableUnits
  cohort_data$EMSEnabledPerc <- cohort_data$EMSEnabledUsers/cohort_data$EMSPaidAvailableUnits
  cohort_data$EMSE5EnabledPerc <- cohort_data$EMSE5EnabledUsers/cohort_data$EMSPaidAvailableUnits
  cohort_data$O365EnabledPerc <- cohort_data$O365EnabledUsers/cohort_data$OfficePaidAvailableUnits
  cohort_data$O365E5EnabledPerc <- cohort_data$O365E5EnabledUsers/cohort_data$OfficePaidAvailableUnits
  cohort_data$M365EnabledPerc <-  cohort_data$M365EnabledUsers/cohort_data$M365PaidAvailableUnits
  cohort_data$EMSPaidTotalPerc <-  (cohort_data$EMSPaidAvailableUnits)/(cohort_data$EMSPaidAvailableUnits + cohort_data$OfficePaidAvailableUnits + cohort_data$WindowsPaidAvailableUnits)
  cohort_data$OfficePaidTotalPerc <-  (cohort_data$OfficePaidAvailableUnits)/(cohort_data$EMSPaidAvailableUnits + cohort_data$OfficePaidAvailableUnits + cohort_data$WindowsPaidAvailableUnits)
  cohort_data$WindowsPaidTotalPerc <- cohort_data$WindowsPaidAvailableUnits/(cohort_data$EMSPaidAvailableUnits + cohort_data$OfficePaidAvailableUnits + cohort_data$WindowsPaidAvailableUnits)
  
  cohort_data$ym <- ym
  
  return(cohort_data)
  
}

elapsed_months <- function(end_date, start_date) {
  ed <- as.POSIXlt(end_date)
  sd <- as.POSIXlt(start_date)
  12 * (ed$year - sd$year) + (ed$mon - sd$mon)
}

vc <- vc_connection('https://cosmos14.osdinfra.net/cosmos/ACE.proc/')
training_cohortq3_path <-  ("local/Projects/M365NCA/TenantProfile/2018-09_TenantProfile.ss")
training_cohortq3 <- ss_all(vc, training_cohortq3_path)
training_cohortq3 <- featurize(training_cohortq3, "2018-09-30", "FY19Q3")

training_cohortq4_path <-  ("local/Projects/M365NCA/TenantProfile/2018-12_TenantProfile.ss")
training_cohortq4 <- ss_all(vc, training_cohortq4_path)
training_cohortq4 <- featurize(training_cohortq4, "2018-12-31", "FY19Q4")

scoring_cohortq1_path <-  ("local/Projects/M365NCA/TenantProfile/2019-03_TenantProfile.ss")
scoring_cohortq1 <- ss_all(vc, scoring_cohortq1_path)
scoring_cohortq1 <- featurize(scoring_cohortq1, "2019-03-31", "FY20Q1")

scoring_cohortq2_path <-  ("local/Projects/M365NCA/TenantProfile/2019-06_TenantProfile.ss")
scoring_cohortq2 <- ss_all(vc, scoring_cohortq2_path)
scoring_cohortq2 <- featurize(scoring_cohortq2, "2019-06-30", "FY20Q2")

training_cohort <- rbind(training_cohortq3, training_cohortq4)
scoring_cohort <- rbind(scoring_cohortq1, scoring_cohortq2)

write.csv(training_cohort, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\TenantProfilev2.csv", row.names = FALSE)
write.csv(scoring_cohort, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\TenantProfilev2.csv", row.names = FALSE)


