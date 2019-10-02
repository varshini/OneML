library(dplyr)
library(readxl)
library(tidyr)
library(data.table)
require(TTR)
require(quantmod)
require(caTools)
options(stringsAsFactors = FALSE)


ProcessOtherRev <- function(data, colname, fy1, fy2, fy3, fy4)
{
  colname1 <- paste(colname, fy1, fy4, sep = '')
  print (colname1)
  fy1 <- paste(colname, fy1, sep ='')
  fy2 <- paste(colname, fy2, sep ='')
  fy3 <- paste(colname, fy3, sep ='')
  fy4 <- paste(colname, fy4, sep ='')
  
  data[[colname1]] <- data[[fy1]] + data[[fy2]] + data[[fy3]] + data[[fy4]]
  return(data)
}

oq1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "17Q1") %>% filter(!is.na(TPID))
oq2 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "17Q2") %>% filter(!is.na(TPID))
oq3 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "17Q3") %>% filter(!is.na(TPID))
oq4 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "17Q4") %>% filter(!is.na(TPID))
oq5 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "18Q1") %>% filter(!is.na(TPID))
oq6 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "18Q2") %>% filter(!is.na(TPID))
oq7 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "18Q3") %>% filter(!is.na(TPID))
oq8 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "18Q4") %>% filter(!is.na(TPID))
oq9 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "19Q1") %>% filter(!is.na(TPID))
oq10 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "19Q2") %>% filter(!is.na(TPID))
oq11 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "19Q3") %>% filter(!is.na(TPID))
oq12 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "19Q4") %>% filter(!is.na(TPID))

other_rev <- 
  full_join(oq1, oq2, by = "TPID") %>%
  full_join(., oq3, by = "TPID") %>%
  full_join(., oq4, by = "TPID") %>%
  full_join(., oq5, by = "TPID") %>%
  full_join(., oq6, by = "TPID") %>% 
  full_join(., oq7, by = "TPID") %>% 
  full_join(., oq8, by = "TPID") %>% 
  full_join(., oq9, by = "TPID") %>% 
  full_join(., oq10, by = "TPID") %>% 
  full_join(., oq11, by = "TPID") %>% 
  full_join(., oq12, by = "TPID")

other_rev[is.na(other_rev)] <- 0

#Featurizing fields which only have two years of data (i.e existed before FY18)

other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreNonM365", "FY17Q2", "FY17Q3", "FY17Q4", "FY18Q1")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreNonM365", "FY18Q2", "FY18Q3", "FY18Q4", "FY19Q1")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreNonM365", "FY17Q3", "FY17Q4", "FY18Q1", "FY18Q2")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreNonM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreNonM365", "FY17Q4", "FY18Q1", "FY18Q2", "FY18Q3")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreNonM365", "FY18Q4", "FY19Q1", "FY19Q2", "FY19Q3")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreNonM365", "FY18Q1", "FY18Q2", "FY18Q3", "FY18Q4")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreNonM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")

other_rev <- ProcessOtherRev(other_rev, "O365CoreNonM365", "FY17Q2", "FY17Q3", "FY17Q4", "FY18Q1")
other_rev <- ProcessOtherRev(other_rev, "O365CoreNonM365", "FY18Q2", "FY18Q3", "FY18Q4", "FY19Q1")
other_rev <- ProcessOtherRev(other_rev, "O365CoreNonM365", "FY17Q3", "FY17Q4", "FY18Q1", "FY18Q2")
other_rev <- ProcessOtherRev(other_rev, "O365CoreNonM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "O365CoreNonM365", "FY17Q4", "FY18Q1", "FY18Q2", "FY18Q3")
other_rev <- ProcessOtherRev(other_rev, "O365CoreNonM365", "FY18Q4", "FY19Q1", "FY19Q2", "FY19Q3")
other_rev <- ProcessOtherRev(other_rev, "O365CoreNonM365", "FY18Q1", "FY18Q2", "FY18Q3", "FY18Q4")
other_rev <- ProcessOtherRev(other_rev, "O365CoreNonM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")

other_rev <- ProcessOtherRev(other_rev, "O365E5NonM365", "FY17Q2", "FY17Q3", "FY17Q4", "FY18Q1")
other_rev <- ProcessOtherRev(other_rev, "O365E5NonM365", "FY18Q2", "FY18Q3", "FY18Q4", "FY19Q1")
other_rev <- ProcessOtherRev(other_rev, "O365E5NonM365", "FY17Q3", "FY17Q4", "FY18Q1", "FY18Q2")
other_rev <- ProcessOtherRev(other_rev, "O365E5NonM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "O365E5NonM365", "FY17Q4", "FY18Q1", "FY18Q2", "FY18Q3")
other_rev <- ProcessOtherRev(other_rev, "O365E5NonM365", "FY18Q4", "FY19Q1", "FY19Q2", "FY19Q3")
other_rev <- ProcessOtherRev(other_rev, "O365E5NonM365", "FY18Q1", "FY18Q2", "FY18Q3", "FY18Q4")
other_rev <- ProcessOtherRev(other_rev, "O365E5NonM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")

other_rev <- ProcessOtherRev(other_rev, "O365E5SkypeforBusinessVoice", "FY17Q2", "FY17Q3", "FY17Q4", "FY18Q1")
other_rev <- ProcessOtherRev(other_rev, "O365E5SkypeforBusinessVoice", "FY18Q2", "FY18Q3", "FY18Q4", "FY19Q1")
other_rev <- ProcessOtherRev(other_rev, "O365E5SkypeforBusinessVoice", "FY17Q3", "FY17Q4", "FY18Q1", "FY18Q2")
other_rev <- ProcessOtherRev(other_rev, "O365E5SkypeforBusinessVoice", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "O365E5SkypeforBusinessVoice", "FY17Q4", "FY18Q1", "FY18Q2", "FY18Q3")
other_rev <- ProcessOtherRev(other_rev, "O365E5SkypeforBusinessVoice", "FY18Q4", "FY19Q1", "FY19Q2", "FY19Q3")
other_rev <- ProcessOtherRev(other_rev, "O365E5SkypeforBusinessVoice", "FY18Q1", "FY18Q2", "FY18Q3", "FY18Q4")
other_rev <- ProcessOtherRev(other_rev, "O365E5SkypeforBusinessVoice", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")

other_rev <- ProcessOtherRev(other_rev, "O365E5SecurityAnalytics", "FY17Q2", "FY17Q3", "FY17Q4", "FY18Q1")
other_rev <- ProcessOtherRev(other_rev, "O365E5SecurityAnalytics", "FY18Q2", "FY18Q3", "FY18Q4", "FY19Q1")
other_rev <- ProcessOtherRev(other_rev, "O365E5SecurityAnalytics", "FY17Q3", "FY17Q4", "FY18Q1", "FY18Q2")
other_rev <- ProcessOtherRev(other_rev, "O365E5SecurityAnalytics", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "O365E5SecurityAnalytics", "FY17Q4", "FY18Q1", "FY18Q2", "FY18Q3")
other_rev <- ProcessOtherRev(other_rev, "O365E5SecurityAnalytics", "FY18Q4", "FY19Q1", "FY19Q2", "FY19Q3")
other_rev <- ProcessOtherRev(other_rev, "O365E5SecurityAnalytics", "FY18Q1", "FY18Q2", "FY18Q3", "FY18Q4")
other_rev <- ProcessOtherRev(other_rev, "O365E5SecurityAnalytics", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")

other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365E3", "FY17Q2", "FY17Q3", "FY17Q4", "FY18Q1")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365E3", "FY18Q2", "FY18Q3", "FY18Q4", "FY19Q1")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365E3", "FY17Q3", "FY17Q4", "FY18Q1", "FY18Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365E3", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365E3", "FY17Q4", "FY18Q1", "FY18Q2", "FY18Q3")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365E3", "FY18Q4", "FY19Q1", "FY19Q2", "FY19Q3")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365E3", "FY18Q1", "FY18Q2", "FY18Q3", "FY18Q4")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365E3", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")

other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365Enterprise", "FY17Q2", "FY17Q3", "FY17Q4", "FY18Q1")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365Enterprise", "FY18Q2", "FY18Q3", "FY18Q4", "FY19Q1")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365Enterprise", "FY17Q3", "FY17Q4", "FY18Q1", "FY18Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365Enterprise", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365Enterprise", "FY17Q4", "FY18Q1", "FY18Q2", "FY18Q3")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365Enterprise", "FY18Q4", "FY19Q1", "FY19Q2", "FY19Q3")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365Enterprise", "FY18Q1", "FY18Q2", "FY18Q3", "FY18Q4")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365Enterprise", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")

other_rev <- ProcessOtherRev(other_rev, "WindowsDeviceLicensing", "FY17Q2", "FY17Q3", "FY17Q4", "FY18Q1")
other_rev <- ProcessOtherRev(other_rev, "WindowsDeviceLicensing", "FY18Q2", "FY18Q3", "FY18Q4", "FY19Q1")
other_rev <- ProcessOtherRev(other_rev, "WindowsDeviceLicensing", "FY17Q3", "FY17Q4", "FY18Q1", "FY18Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsDeviceLicensing", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsDeviceLicensing", "FY17Q4", "FY18Q1", "FY18Q2", "FY18Q3")
other_rev <- ProcessOtherRev(other_rev, "WindowsDeviceLicensing", "FY18Q4", "FY19Q1", "FY19Q2", "FY19Q3")
other_rev <- ProcessOtherRev(other_rev, "WindowsDeviceLicensing", "FY18Q1", "FY18Q2", "FY18Q3", "FY18Q4")
other_rev <- ProcessOtherRev(other_rev, "WindowsDeviceLicensing", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")

featurize <- function (data, colname, yr1, yr2, outputCol1, outputCol2, outputCol3, outputCol4, fy)
{
  yr1 <- paste(colname, yr1, sep='')
  yr2 <- paste(colname, yr2, sep='')
  outputCol1 <- paste(colname, outputCol1, sep = "")
  outputCol2 <- paste(colname, outputCol2, sep = "")
  outputCol3 <- paste(colname, outputCol3, sep = "")
  outputCol4 <- paste(colname, outputCol4, sep = "")
  
  # YOY change 
  data[[outputCol1]] = 
    ifelse(
      data[[yr1]] <= 0,
      ifelse(
        data[[yr2]] <= 0,
        0,
        ( data[[yr2]] / 1 - 1 )
      ),
      ( ifelse( data[[yr2]] > 0, data[[yr2]], 0 ) / ifelse( data[[yr1]] > 0, data[[yr1]], 0 ) - 1 )
    )
  
  # Mean 
  data[[outputCol2]] <- (data[[yr1]] + data[[yr2]])/2
  
  #Actuals 
  data[[outputCol3]] <- data[[yr1]]
  data[[outputCol4]] <- data[[yr2]]
  data[['ym']] <- fy
  return(data)
}

# Compute features  
training_cohort_q3 <- featurize(other_rev, "EnterpriseMobilityCoreNonM365", "FY17Q2FY18Q1", "FY18Q2FY19Q1", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q3") %>% select(TPID, EnterpriseMobilityCoreNonM365YoYChange, EnterpriseMobilityCoreNonM365Mean, EnterpriseMobilityCoreNonM365Yr1, EnterpriseMobilityCoreNonM365Yr2, ym)
training_cohort_q4 <- featurize(other_rev, "EnterpriseMobilityCoreNonM365", "FY17Q3FY18Q2", "FY18Q3FY19Q2", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q4") %>% select(TPID, EnterpriseMobilityCoreNonM365YoYChange, EnterpriseMobilityCoreNonM365Mean, EnterpriseMobilityCoreNonM365Yr1, EnterpriseMobilityCoreNonM365Yr2, ym)
scoring_cohort_q1 <- featurize(other_rev, "EnterpriseMobilityCoreNonM365", "FY17Q4FY18Q3", "FY18Q4FY19Q3", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q1") %>% select(TPID, EnterpriseMobilityCoreNonM365YoYChange, EnterpriseMobilityCoreNonM365Mean, EnterpriseMobilityCoreNonM365Yr1, EnterpriseMobilityCoreNonM365Yr2, ym)
scoring_cohort_q2 <- featurize(other_rev, "EnterpriseMobilityCoreNonM365", "FY18Q1FY18Q4", "FY19Q1FY19Q4", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q2") %>% select(TPID, EnterpriseMobilityCoreNonM365YoYChange, EnterpriseMobilityCoreNonM365Mean, EnterpriseMobilityCoreNonM365Yr1, EnterpriseMobilityCoreNonM365Yr2, ym)
training_cohort_1 <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_1 <- rbind(scoring_cohort_q1, scoring_cohort_q2)

training_cohort_q3 <- featurize(other_rev, "O365CoreNonM365", "FY17Q2FY18Q1", "FY18Q2FY19Q1", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q3") %>% select(TPID, O365CoreNonM365YoYChange, O365CoreNonM365Mean, O365CoreNonM365Yr1, O365CoreNonM365Yr2, ym)
training_cohort_q4 <- featurize(other_rev, "O365CoreNonM365", "FY17Q3FY18Q2", "FY18Q3FY19Q2", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q4") %>% select(TPID, O365CoreNonM365YoYChange, O365CoreNonM365Mean, O365CoreNonM365Yr1, O365CoreNonM365Yr2, ym)
scoring_cohort_q1 <- featurize(other_rev, "O365CoreNonM365", "FY17Q4FY18Q3", "FY18Q4FY19Q3", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q1") %>% select(TPID, O365CoreNonM365YoYChange, O365CoreNonM365Mean, O365CoreNonM365Yr1, O365CoreNonM365Yr2, ym)
scoring_cohort_q2 <- featurize(other_rev, "O365CoreNonM365", "FY18Q1FY18Q4", "FY19Q1FY19Q4", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q2") %>% select(TPID, O365CoreNonM365YoYChange, O365CoreNonM365Mean, O365CoreNonM365Yr1, O365CoreNonM365Yr2, ym)
training_cohort_2 <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_2 <- rbind(scoring_cohort_q1, scoring_cohort_q2)

training_cohort_q3 <- featurize(other_rev, "O365E5NonM365", "FY17Q2FY18Q1", "FY18Q2FY19Q1", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q3") %>% select(TPID, O365E5NonM365YoYChange, O365E5NonM365Mean, O365E5NonM365Yr1, O365E5NonM365Yr2, ym)
training_cohort_q4 <- featurize(other_rev, "O365E5NonM365", "FY17Q3FY18Q2", "FY18Q3FY19Q2", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q4") %>% select(TPID, O365E5NonM365YoYChange, O365E5NonM365Mean, O365E5NonM365Yr1, O365E5NonM365Yr2, ym)
scoring_cohort_q1 <- featurize(other_rev, "O365E5NonM365", "FY17Q4FY18Q3", "FY18Q4FY19Q3", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q1") %>% select(TPID, O365E5NonM365YoYChange, O365E5NonM365Mean, O365E5NonM365Yr1, O365E5NonM365Yr2, ym)
scoring_cohort_q2 <- featurize(other_rev, "O365E5NonM365", "FY18Q1FY18Q4", "FY19Q1FY19Q4", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q2") %>% select(TPID, O365E5NonM365YoYChange, O365E5NonM365Mean, O365E5NonM365Yr1, O365E5NonM365Yr2, ym)
training_cohort_3 <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_3 <- rbind(scoring_cohort_q1, scoring_cohort_q2)

training_cohort_q3 <- featurize(other_rev, "O365E5SkypeforBusinessVoice", "FY17Q2FY18Q1", "FY18Q2FY19Q1", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q3") %>% select(TPID, O365E5SkypeforBusinessVoiceYoYChange, O365E5SkypeforBusinessVoiceMean, O365E5SkypeforBusinessVoiceYr1, O365E5SkypeforBusinessVoiceYr2, ym)
training_cohort_q4 <- featurize(other_rev, "O365E5SkypeforBusinessVoice", "FY17Q3FY18Q2", "FY18Q3FY19Q2", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q4") %>% select(TPID, O365E5SkypeforBusinessVoiceYoYChange, O365E5SkypeforBusinessVoiceMean, O365E5SkypeforBusinessVoiceYr1, O365E5SkypeforBusinessVoiceYr2, ym)
scoring_cohort_q1 <- featurize(other_rev, "O365E5SkypeforBusinessVoice", "FY17Q4FY18Q3", "FY18Q4FY19Q3", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q1") %>% select(TPID, O365E5SkypeforBusinessVoiceYoYChange, O365E5SkypeforBusinessVoiceMean, O365E5SkypeforBusinessVoiceYr1, O365E5SkypeforBusinessVoiceYr2, ym)
scoring_cohort_q2 <- featurize(other_rev, "O365E5SkypeforBusinessVoice", "FY18Q1FY18Q4", "FY19Q1FY19Q4", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q2") %>% select(TPID, O365E5SkypeforBusinessVoiceYoYChange, O365E5SkypeforBusinessVoiceMean, O365E5SkypeforBusinessVoiceYr1, O365E5SkypeforBusinessVoiceYr2, ym)
training_cohort_4 <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_4 <- rbind(scoring_cohort_q1, scoring_cohort_q2)

training_cohort_q3 <- featurize(other_rev, "O365E5SecurityAnalytics", "FY17Q2FY18Q1", "FY18Q2FY19Q1", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q3") %>% select(TPID, O365E5SecurityAnalyticsYoYChange, O365E5SecurityAnalyticsMean, O365E5SecurityAnalyticsYr1, O365E5SecurityAnalyticsYr2, ym)
training_cohort_q4 <- featurize(other_rev, "O365E5SecurityAnalytics", "FY17Q3FY18Q2", "FY18Q3FY19Q2", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q4") %>% select(TPID, O365E5SecurityAnalyticsYoYChange, O365E5SecurityAnalyticsMean, O365E5SecurityAnalyticsYr1, O365E5SecurityAnalyticsYr2, ym)
scoring_cohort_q1 <- featurize(other_rev, "O365E5SecurityAnalytics", "FY17Q4FY18Q3", "FY18Q4FY19Q3", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q1") %>% select(TPID, O365E5SecurityAnalyticsYoYChange, O365E5SecurityAnalyticsMean, O365E5SecurityAnalyticsYr1, O365E5SecurityAnalyticsYr2, ym)
scoring_cohort_q2 <- featurize(other_rev, "O365E5SecurityAnalytics", "FY18Q1FY18Q4", "FY19Q1FY19Q4", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q2") %>% select(TPID, O365E5SecurityAnalyticsYoYChange, O365E5SecurityAnalyticsMean, O365E5SecurityAnalyticsYr1, O365E5SecurityAnalyticsYr2, ym)
training_cohort_5 <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_5 <- rbind(scoring_cohort_q1, scoring_cohort_q2)

training_cohort_q3 <- featurize(other_rev, "WindowsCoreNonM365E3", "FY17Q2FY18Q1", "FY18Q2FY19Q1", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q3") %>% select(TPID, WindowsCoreNonM365E3YoYChange, WindowsCoreNonM365E3Mean, WindowsCoreNonM365E3Yr1, WindowsCoreNonM365E3Yr2, ym)
training_cohort_q4 <- featurize(other_rev, "WindowsCoreNonM365E3", "FY17Q3FY18Q2", "FY18Q3FY19Q2", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q4") %>% select(TPID, WindowsCoreNonM365E3YoYChange, WindowsCoreNonM365E3Mean, WindowsCoreNonM365E3Yr1, WindowsCoreNonM365E3Yr2, ym)
scoring_cohort_q1 <- featurize(other_rev, "WindowsCoreNonM365E3", "FY17Q4FY18Q3", "FY18Q4FY19Q3", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q1") %>% select(TPID, WindowsCoreNonM365E3YoYChange, WindowsCoreNonM365E3Mean, WindowsCoreNonM365E3Yr1, WindowsCoreNonM365E3Yr2, ym)
scoring_cohort_q2 <- featurize(other_rev, "WindowsCoreNonM365E3", "FY18Q1FY18Q4", "FY19Q1FY19Q4", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q2") %>% select(TPID, WindowsCoreNonM365E3YoYChange, WindowsCoreNonM365E3Mean, WindowsCoreNonM365E3Yr1, WindowsCoreNonM365E3Yr2, ym)
training_cohort_6 <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_6 <- rbind(scoring_cohort_q1, scoring_cohort_q2)

training_cohort_q3 <- featurize(other_rev, "WindowsCoreNonM365Enterprise", "FY17Q2FY18Q1", "FY18Q2FY19Q1", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q3") %>% select(TPID, WindowsCoreNonM365EnterpriseYoYChange, WindowsCoreNonM365EnterpriseMean, WindowsCoreNonM365EnterpriseYr1, WindowsCoreNonM365EnterpriseYr2, ym)
training_cohort_q4 <- featurize(other_rev, "WindowsCoreNonM365Enterprise", "FY17Q3FY18Q2", "FY18Q3FY19Q2", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q4") %>% select(TPID, WindowsCoreNonM365EnterpriseYoYChange, WindowsCoreNonM365EnterpriseMean, WindowsCoreNonM365EnterpriseYr1, WindowsCoreNonM365EnterpriseYr2, ym)
scoring_cohort_q1 <- featurize(other_rev, "WindowsCoreNonM365Enterprise", "FY17Q4FY18Q3", "FY18Q4FY19Q3", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q1") %>% select(TPID, WindowsCoreNonM365EnterpriseYoYChange, WindowsCoreNonM365EnterpriseMean, WindowsCoreNonM365EnterpriseYr1, WindowsCoreNonM365EnterpriseYr2, ym)
scoring_cohort_q2 <- featurize(other_rev, "WindowsCoreNonM365Enterprise", "FY18Q1FY18Q4", "FY19Q1FY19Q4", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q2") %>% select(TPID, WindowsCoreNonM365EnterpriseYoYChange, WindowsCoreNonM365EnterpriseMean, WindowsCoreNonM365EnterpriseYr1, WindowsCoreNonM365EnterpriseYr2, ym)
training_cohort_7 <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_7 <- rbind(scoring_cohort_q1, scoring_cohort_q2)

training_cohort_q3 <- featurize(other_rev, "WindowsDeviceLicensing", "FY17Q2FY18Q1", "FY18Q2FY19Q1", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q3") %>% select(TPID, WindowsDeviceLicensingYoYChange, WindowsDeviceLicensingMean, WindowsDeviceLicensingYr1, WindowsDeviceLicensingYr2, ym)
training_cohort_q4 <- featurize(other_rev, "WindowsDeviceLicensing", "FY17Q3FY18Q2", "FY18Q3FY19Q2", "YoYChange", "Mean", "Yr1", "Yr2", "FY19Q4") %>% select(TPID, WindowsDeviceLicensingYoYChange, WindowsDeviceLicensingMean, WindowsDeviceLicensingYr1, WindowsDeviceLicensingYr2, ym)
scoring_cohort_q1 <- featurize(other_rev, "WindowsDeviceLicensing", "FY17Q4FY18Q3", "FY18Q4FY19Q3", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q1") %>% select(TPID, WindowsDeviceLicensingYoYChange, WindowsDeviceLicensingMean, WindowsDeviceLicensingYr1, WindowsDeviceLicensingYr2, ym)
scoring_cohort_q2 <- featurize(other_rev, "WindowsDeviceLicensing", "FY18Q1FY18Q4", "FY19Q1FY19Q4", "YoYChange", "Mean", "Yr1", "Yr2", "FY20Q2") %>% select(TPID, WindowsDeviceLicensingYoYChange, WindowsDeviceLicensingMean, WindowsDeviceLicensingYr1, WindowsDeviceLicensingYr2, ym)
training_cohort_8 <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_8 <- rbind(scoring_cohort_q1, scoring_cohort_q2)

revenue_training_data <- full_join(training_cohort_1, training_cohort_2, by = c("TPID", "ym")) %>% 
  full_join(., training_cohort_3, by = c("TPID", "ym")) %>% 
  full_join(., training_cohort_4, by = c("TPID", "ym")) %>% 
  full_join(., training_cohort_5, by = c("TPID", "ym")) %>% 
  full_join(., training_cohort_6, by = c("TPID", "ym")) %>% 
  full_join(., training_cohort_7, by = c("TPID", "ym")) %>% 
  full_join(., training_cohort_8, by = c("TPID", "ym"))

revenue_scoring_data <- full_join(scoring_cohort_1, scoring_cohort_2, by = c("TPID", "ym")) %>% 
  full_join(., scoring_cohort_3, by = c("TPID", "ym")) %>% 
  full_join(., scoring_cohort_4, by = c("TPID", "ym")) %>% 
  full_join(., scoring_cohort_5, by = c("TPID", "ym")) %>% 
  full_join(., scoring_cohort_6, by = c("TPID", "ym")) %>% 
  full_join(., scoring_cohort_7, by = c("TPID", "ym")) %>% 
  full_join(., scoring_cohort_8, by = c("TPID", "ym"))

write.csv(revenue_training_data, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\OtherRevenue.csv", row.names = FALSE)
write.csv(revenue_scoring_data, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\OtherRevenue.csv", row.names = FALSE)

