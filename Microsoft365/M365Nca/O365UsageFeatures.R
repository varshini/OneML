## Featurize Office Usage variables ## 
library(CosmosToR)

Trending <- function(x){
  n <- length(x)
  if(n<4 || sum(x>0)<4 || any(is.na(x))) return(rep(0, n))
  cts <- caTools::runmean(x, 3, align = 'right')
  ROC(cts, 1, type = "discrete")
}


vc <- vc_connection('https://cosmos14.osdinfra.net/cosmos/ACE.proc/')
o365usage_path <-  ("/local/Projects/M365NCA/O365MAU/O365MAUAggregated.ss")
o365usage <- ss_all(vc, o365usage_path)
o365usage$ym <- format(as.Date(o365usage$SnapshotDate, "%m/%d/%Y %H:%M:%S"), "%Y%m")

proplususage_path <-  ("/local/Projects/M365NCA/O365MAU/ProPlusMAUAggregated.ss")
proplususage <- ss_all(vc, proplususage_path)
proplususage$ym <- format(as.Date(proplususage$Date, "%m/%d/%Y %H:%M:%S"), "%Y%m")

#Make a final variable for all O365 usage 
o365usage <- left_join(o365usage, proplususage, by = c("FinalTPID", "ym"))
o365usage[is.na(o365usage)] <- 0

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

emsusage_path <-  ("/local/Projects/M365NCA/EMSUsage/EMSMAUAggregated.ss")
emsUsage <- ss_all(vc, emsusage_path)

# Features - 1) Mean 2) Trend 3) Actual Value at the end of training snapshot
featurizeEMSUsage <- function(cohort_data, cohort_ym, fy)
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




