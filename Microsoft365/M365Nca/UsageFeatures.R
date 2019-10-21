## Featurize Office Usage variables ## 
library(CosmosToR)
library(dplyr)
require(TTR)
require(quantmod)
require(caTools)


Trending <- function(x){
  x[ x == 0 ] <- 1
  n <- length(x)
  if(n<4 || sum(x>0)<4 || any(is.na(x))) return(rep(0, n))
  cts <- caTools::runmean(x, 3, align = 'right')
  ROC(cts, 1, type = "discrete")
}


# vc <- vc_connection('https://cosmos14.osdinfra.net/cosmos/ACE.proc/')
# o365usage_path <-  ("/local/Projects/M365NCA/O365MAU/O365MAUAggregated.ss")
# o365usage <- ss_all(vc, o365usage_path)
o365usage <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\MAU\\O365MAUAggregated.csv")
o365usage$ym <- format(as.Date(o365usage$SnapshotDate, "%m/%d/%Y %H:%M:%S"), "%Y%m")

# proplususage_path <-  ("/local/Projects/M365NCA/O365MAU/ProPlusMAUAggregated.ss")
# proplususage <- ss_all(vc, proplususage_path)
proplususage <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\MAU\\ProPlusMAUAggregated.csv")
proplususage$ym <- format(as.Date(proplususage$Date, "%m/%d/%Y %H:%M:%S"), "%Y%m")

#Make a final variable for all O365 usage 
o365usage <- full_join(o365usage, proplususage, by = c("FinalTPID", "ym"))
o365usage[is.na(o365usage)] <- 0
#o365usage[ o365usage == 0 ] <- 1

# Features - 1) Mean 2) Trend 3) Actual Value at the end of training snapshot
featurizeOfficeUsage <- function(cohort_data, cohort_ym, fy)
{
  cohort_data <- cohort_data %>% filter(ym <= cohort_ym)

  features <- cohort_data %>% 
              group_by(FinalTPID) %>% 
              arrange(ym) %>% 
              mutate(O365MauTrend = Trending(O365MAU),
                     O365MauMean = mean(O365MAU),
                     EXOMAUTrend = Trending(EXOMAU),
                     EXOMAUMean = mean(EXOMAU),
                     ODSPMAUTrend = Trending(ODSPMAU),
                     ODSPMAUMean = mean(ODSPMAU),
                     SfBMAUTrend = Trending(SfBMAU),
                     SfBMAUMean = mean(SfBMAU),
                     YammerMAUTrend = Trending(YammerMAU),
                     YammerMAUMean = mean(YammerMAU),
                     TeamsMAUTrend = Trending(TeamsMAU),
                     TeamsMAUMean = mean(TeamsMAU),
                     ProPlusMAUTrend = Trending(ProPlusMAU),
                     ProPlusMAUMean = mean(ProPlusMAU),
                     OutlookMAUTrend = Trending(OutlookMAU),
                     OutlookMAUMean = mean(OutlookMAU),
                     OutlookMobileTrend = Trending(OutlookMAU_Mobile),
                     OutlookMobileMean = mean(OutlookMAU_Mobile)
                    )
  
  features <- features %>% filter(ym == cohort_ym)
  features <- features %>% select(FinalTPID, ym, O365MAU, O365MauTrend, O365MauMean, EXOMAU, EXOMAUTrend, EXOMAUMean,
                                  ODSPMAU, ODSPMAUTrend, ODSPMAUMean, SfBMAU, SfBMAUTrend, SfBMAUMean, YammerMAU, YammerMAUTrend, YammerMAUMean,  
                                  TeamsMAU, TeamsMAUTrend, TeamsMAUMean, ProPlusMAU, ProPlusMAUTrend, ProPlusMAUMean, 
                                  OutlookMAU, OutlookMAUTrend, OutlookMAUMean,OutlookMAU_Mobile, OutlookMobileTrend, OutlookMobileMean)
  
  pro_plus_data <- cohort_data %>% filter(ym >= "201802" & ym <= cohort_ym)
  pro_plus_data <- pro_plus_data %>% 
    group_by(FinalTPID) %>% 
    arrange(ym) %>% 
    mutate(ProPlusPerpetualTrend = Trending(DesktopPerpetual),
           ProPlusPerpetualMean = mean(DesktopPerpetual),
           ProPlusOnlineTrend = Trending(DesktopSubscription),
           ProPlusOnlineMean = mean(DesktopSubscription))
  
  pro_plus_data <- pro_plus_data %>% filter(ym == cohort_ym)
  pro_plus_data <- pro_plus_data %>% select(FinalTPID, ym, DesktopPerpetual, ProPlusPerpetualTrend, ProPlusPerpetualMean,
                                            DesktopSubscription, ProPlusOnlineTrend, ProPlusOnlineMean)
  
  features <- inner_join(features, pro_plus_data, by = c("FinalTPID","ym"))
  features$ym <- fy
  
  return (features)
}


training_cohort_q3 <- featurizeOfficeUsage(o365usage, "201809", "FY19Q3")
training_cohort_q4 <- featurizeOfficeUsage(o365usage, "201812", "FY19Q4")
training_cohort <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_q1 <- featurizeOfficeUsage(o365usage, "201903", "FY20Q1")
scoring_cohort_q2 <- featurizeOfficeUsage(o365usage, "201906", "FY20Q2")
scoring_cohort <- rbind(scoring_cohort_q1, scoring_cohort_q2)

write.csv(training_cohort, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\O365Usage.csv", row.names = FALSE)
write.csv(scoring_cohort, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\O365Usage.csv", row.names = FALSE)

## EMS USAGE ##

# Changing the trending function for EMS since I have data for only after 201807. So making it a running mean over 1 month trends

EMSTrending <- function(x){
  x[ x == 0 ] <- 1
  n <- length(x)
  if(n<2 || sum(x>0)<2 || any(is.na(x))) return(rep(0, n))
  cts <- caTools::runmean(x, 1, align = 'right')
  ROC(cts, 1, type = "discrete")
}

# emsusage_path <-  ("/local/Projects/M365NCA/EMSUsage/EMSMAUAggregated.ss")
# emsUsage <- ss_all(vc, emsusage_path)
emsUsage <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\MAU\\EMSMAUAggregated.csv")
emsUsage$ym <- format(as.Date(emsUsage$SnapshotDate, "%m/%d/%Y %H:%M:%S"), "%Y%m")

emsusage_intune <- emsUsage %>% filter(!is.na(FinalTPIDintune)) %>%
                    select(ym, FinalTPIDintune, Intune_configured_users, Intune_active_users) %>% rename(FinalTPID = FinalTPIDintune)
emsusage_aadp <- emsUsage %>% filter(!is.na(FinalTPIDaadp)) %>%
                    select(ym, FinalTPIDaadp, AADP_configured_users, AADP_active_users) %>% rename(FinalTPID = FinalTPIDaadp)
emsusage_oatp <- emsUsage %>% filter(!is.na(FinalTPIDoatp)) %>%
                    select(ym, FinalTPIDoatp, AIP_configured_users, AIP_active_users) %>% rename(FinalTPID = FinalTPIDoatp)
emsusage_aip <- emsUsage %>% filter(!is.na(FinalTPIDaip)) %>% 
                    select(ym, FinalTPIDaip, OATP_active_users) %>% rename(FinalTPID = FinalTPIDaip)

emsUsage <- emsusage_intune %>% 
            full_join(., emsusage_aadp, by = c("FinalTPID", "ym")) %>% 
            full_join(., emsusage_oatp, by = c("FinalTPID", "ym")) %>%
            full_join(., emsusage_aip, by = c("FinalTPID", "ym"))

emsUsage[is.na(emsUsage)] <- 0
#emsUsage[ emsUsage == 0 ] <- 1

# Features - 1) Mean 2) Trend 3) Actual Value at the end of training snapshot
featurizeEMSUsage <- function(cohort_data, cohort_ym, fy)
{
    cohort_data <- emsUsage
    
    cohort_data <- cohort_data %>% filter(ym <= cohort_ym)
    features <- cohort_data %>% 
    group_by(FinalTPID) %>% 
    arrange(ym) %>% 
    mutate(IntuneConfiguredUsersTrend = EMSTrending(Intune_configured_users),
           IntuneConfiguredUsersMean = mean(Intune_configured_users),
           IntuneActiveUsersTrend = EMSTrending(Intune_active_users),
           IntuneActiveUsersMean = mean(Intune_active_users),
           AADPConfiguredUsersTrend = EMSTrending(AADP_configured_users),
           AADPConfiguredUsersMean = mean(AADP_configured_users),
           AADPActiveUsersTrend = EMSTrending(AADP_active_users),
           AADPActiveUsersMean = mean(AADP_active_users),
           AIPConfiguredUsersTrend = EMSTrending(AIP_configured_users),
           AIPConfiguredUsersMean = mean(AIP_configured_users),
           AIPActiveUsersTrend = EMSTrending(AIP_active_users),
           AIPActiveUsersMean = mean(AIP_active_users),
           OATPActiveUsersTrend = EMSTrending(OATP_active_users),
           OATPActiveUsersMean = mean(OATP_active_users)
          )
  
  features <- features %>% filter(ym == cohort_ym)
  features <- features %>% select(FinalTPID, ym, Intune_configured_users, IntuneConfiguredUsersTrend, IntuneConfiguredUsersMean,
                                  Intune_active_users, IntuneActiveUsersTrend, IntuneActiveUsersMean, 
                                  AADP_configured_users, AADPConfiguredUsersTrend, AADPConfiguredUsersMean,
                                  AADP_active_users, AADPActiveUsersTrend, AADPActiveUsersMean,
                                  AIP_configured_users, AIPConfiguredUsersTrend, AIPConfiguredUsersMean,
                                  AIP_active_users, AIPActiveUsersTrend, AIPActiveUsersMean,
                                  OATP_active_users, OATPActiveUsersTrend, OATPActiveUsersMean)
  
  features$ym <- fy
  
  return (features)
}

training_cohort_q3 <- featurizeEMSUsage(emsUsage, "201809", "FY19Q3")
training_cohort_q4 <- featurizeEMSUsage(emsUsage, "201812", "FY19Q4")
training_cohort <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_q1 <- featurizeEMSUsage(emsUsage, "201903", "FY20Q1")
scoring_cohort_q2 <- featurizeEMSUsage(emsUsage, "201906", "FY20Q2")
scoring_cohort <- rbind(scoring_cohort_q1, scoring_cohort_q2)

write.csv(training_cohort, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\EMSUsage.csv", row.names = FALSE)
write.csv(scoring_cohort, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\EMSUsage.csv", row.names = FALSE)

## Collaboration Features ##

featurizeCollaboration <- function(cohort_data, fy)
{
  cohort_data$NumAttachmentsSent <- cohort_data$NumWordStreamAttachmentsSent + cohort_data$NumExcelStreamAttachmentsSent +
    cohort_data$NumPowerPointStreamAttachmentsSent + cohort_data$NumPDFStreamAttachmentsSent + cohort_data$NumOneNoteStreamAttachmentsSent + 
    cohort_data$NumWordReferenceAttachmentsSent+ cohort_data$NumExcelReferenceAttachmentsSent + cohort_data$NumPowerPointReferenceAttachmentsSent +
    cohort_data$NumPDFReferenceAttachmentsSent + cohort_data$NumOneNoteReferenceAttachmentsSent
  
  cohort_data$NumAttachmentsOpened <- cohort_data$NumWordStreamAttachmentOpened + cohort_data$NumExcelStreamAttachmentOpened +
    cohort_data$NumPowerPointStreamAttachmentOpened + cohort_data$NumPDFStreamAttachmentOpened + cohort_data$NumOneNoteStreamAttachmentOpened +        
    cohort_data$NumWordReferenceAttachmentOpened + cohort_data$NumExcelReferenceAttachmentOpened +  cohort_data$NumPowerPointReferenceAttachmentOpened +  
     cohort_data$NumPDFReferenceAttachmentOpened + cohort_data$NumOneNoteReferenceAttachmentOpened  
  
  cohort_data <- cohort_data %>% dplyr::select(FinalTPID, ym, NumAttachmentsSent, NumAttachmentsOpened, NumEmailsSent, NumInboundEmailsReceived, 
                                        NumInboundEmailsRead, NumEmailsInWhichOpenedAttachments, 
                                        NumWXPOUsers, NumWXPOEditors, NumWXPOCoauthors, NumWXPOUsersCollaborated)
  
  cohort_data$ym <- fy
  return (cohort_data)
}

collaboration_path_training_q3 <-  ("/local/Projects/M365NCA/Collaboration/2018-09_EXOUsage.ss")
collaboration_training_q3_exo <- ss_all(vc, collaboration_path_training_q3)
collaboration_path_training_q3 <-  ("/local/Projects/M365NCA/Collaboration/2018-09_SPOUsage.ss")
collaboration_training_q3_spo <- ss_all(vc, collaboration_path_training_q3)
collaboration_training_q3 <- full_join(collaboration_training_q3_exo, collaboration_training_q3_spo, by = c("FinalTPID", "SnapshotDate"))
collaboration_training_q3$ym <- format(as.Date(collaboration_training_q3$SnapshotDate, "%m/%d/%Y %H:%M:%S"), "%Y%m")
training_cohort_q3 <- featurizeCollaboration(collaboration_training_q3,"FY19Q3")

collaboration_path_training_q4 <-  ("/local/Projects/M365NCA/Collaboration/2018-12_EXOUsage.ss")
collaboration_training_q4_exo <- ss_all(vc, collaboration_path_training_q4)
collaboration_path_training_q4 <-  ("/local/Projects/M365NCA/Collaboration/2018-12_SPOUsage.ss")
collaboration_training_q4_spo <- ss_all(vc, collaboration_path_training_q4)
collaboration_training_q4 <- full_join(collaboration_training_q4_exo, collaboration_training_q4_spo, by = c("FinalTPID", "SnapshotDate"))
collaboration_training_q4$ym <- format(as.Date(collaboration_training_q4$SnapshotDate, "%m/%d/%Y %H:%M:%S"), "%Y%m")
training_cohort_q4 <- featurizeCollaboration(collaboration_training_q4,"FY19Q4")

training_cohort <- rbind(training_cohort_q3, training_cohort_q4)

collaboration_path_scoring_q1 <-  ("/local/Projects/M365NCA/Collaboration/2019-03_EXOUsage.ss")
collaboration_scoring_q1_exo <- ss_all(vc, collaboration_path_scoring_q1)
collaboration_path_scoring_q1 <-  ("/local/Projects/M365NCA/Collaboration/2019-03_SPOUsage.ss")
collaboration_scoring_q1_spo <- ss_all(vc, collaboration_path_scoring_q1)
collaboration_scoring_q1 <- full_join(collaboration_scoring_q1_exo, collaboration_scoring_q1_spo, by = c("FinalTPID", "SnapshotDate"))
collaboration_scoring_q1$ym <- format(as.Date(collaboration_scoring_q1$SnapshotDate, "%m/%d/%Y %H:%M:%S"), "%Y%m")
scoring_cohort_q1 <- featurizeCollaboration(collaboration_scoring_q1,"FY20Q1")

collaboration_path_scoring_q2 <-  ("/local/Projects/M365NCA/Collaboration/2019-06_EXOUsage.ss")
collaboration_scoring_q2_exo <- ss_all(vc, collaboration_path_scoring_q2)
collaboration_path_scoring_q2 <-  ("/local/Projects/M365NCA/Collaboration/2019-06_SPOUsage.ss")
collaboration_scoring_q2_spo <- ss_all(vc, collaboration_path_scoring_q2)
collaboration_scoring_q2 <- full_join(collaboration_scoring_q2_exo, collaboration_scoring_q2_spo, by = c("FinalTPID", "SnapshotDate"))
collaboration_scoring_q2$ym <- format(as.Date(collaboration_scoring_q2$SnapshotDate, "%m/%d/%Y %H:%M:%S"), "%Y%m")
scoring_cohort_q2 <- featurizeCollaboration(collaboration_scoring_q2,"FY20Q1")

scoring_cohort <- rbind(scoring_cohort_q1, scoring_cohort_q2)

write.csv(training_cohort, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\Collaboration.csv", row.names = FALSE)
write.csv(scoring_cohort, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\Collaboration.csv.csv", row.names = FALSE)
