library(readxl)
library(dplyr)
library(tidyverse)
options(stringsAsFactors = FALSE)

featurizeRev <- function (data, yr1, yr2, outputCol1, outputCol2, outputCol3, outputCol4, ym)
{
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
  data$ym <- ym
  
  return(data)
}

#Azure Rev 
azure <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\Azure.xlsx", sheet = "Sheet3")
azure[is.na(azure)] <- 0

azure$y18q3y19q2 <- azure$`FY18-Q3`+ azure$`FY18-Q4`+ azure$`FY19-Q1` + azure$`FY19-Q2`
azure$y19q3y20q2 <- azure$`FY19-Q3` + azure$`FY19-Q4`+ azure$`FY20-Q1`+ azure$`FY20-Q2`

azure$y19q1y19q4 <- azure$`FY19-Q1`+ azure$`FY19-Q2`+ azure$`FY19-Q3` + azure$`FY19-Q4`
azure$y20q1y20q4 <- azure$`FY20-Q1` + azure$`FY20-Q2`+ azure$`FY20-Q3`+ azure$`FY20-Q4`


training_azure <- featurizeRev(azure, "y18q3y19q2", "y19q3y20q2", "AzureYoYChange", "AzureMean", "AzureYr1", "AzureYr2", "201912") %>% select(FinalTPID, AzureYoYChange, AzureMean, AzureYr1, AzureYr2, ym)
scoring_azure <- featurizeRev(azure, "y19q1y19q4", "y20q1y20q4", "AzureYoYChange", "AzureMean", "AzureYr1", "AzureYr2", "202006") %>% select(FinalTPID, AzureYoYChange, AzureMean, AzureYr1, AzureYr2, ym)


#Dynamics Rev
dynamics <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\Dynamics.xlsx", sheet = "csv")
dynamics[is.na(dynamics)] <- 0

dynamics$y18q3y19q2 <- dynamics$`FY18-Q3`+ dynamics$`FY18-Q4`+ dynamics$`FY19-Q1` + dynamics$`FY19-Q2`
dynamics$y19q3y20q2 <- dynamics$`FY19-Q3` + dynamics$`FY19-Q4`+ dynamics$`FY20-Q1`+ dynamics$`FY20-Q2`

dynamics$y19q1y19q4 <- dynamics$`FY19-Q1`+ dynamics$`FY19-Q2`+ dynamics$`FY19-Q3` + dynamics$`FY19-Q4`
dynamics$y20q1y20q4 <- dynamics$`FY20-Q1` + dynamics$`FY20-Q2`+ dynamics$`FY20-Q3`+ dynamics$`FY20-Q4`


training_dynamics <- featurizeRev(dynamics, "y18q3y19q2", "y19q3y20q2", "dynamicsYoYChange", "dynamicsMean", "dynamicsYr1", "dynamicsYr2", "201912") %>% select(FinalTPID, dynamicsYoYChange, dynamicsMean, dynamicsYr1, dynamicsYr2, ym)
scoring_dynamics <- featurizeRev(dynamics, "y19q1y19q4", "y20q1y20q4", "dynamicsYoYChange", "dynamicsMean", "dynamicsYr1", "dynamicsYr2", "202006") %>% select(FinalTPID, dynamicsYoYChange, dynamicsMean, dynamicsYr1, dynamicsYr2, ym)


#OnPrem Rev
onprem <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\OnPrem.xlsx", sheet = "Sheet2")
onprem[is.na(onprem)] <- 0

onprem$y18q3y19q2 <- onprem$`FY18-Q3`+ onprem$`FY18-Q4`+ onprem$`FY19-Q1` + onprem$`FY19-Q2`
onprem$y19q3y20q2 <- onprem$`FY19-Q3` + onprem$`FY19-Q4`+ onprem$`FY20-Q1`+ onprem$`FY20-Q2`

onprem$y19q1y19q4 <- onprem$`FY19-Q1`+ onprem$`FY19-Q2`+ onprem$`FY19-Q3` + onprem$`FY19-Q4`
onprem$y20q1y20q4 <- onprem$`FY20-Q1` + onprem$`FY20-Q2`+ onprem$`FY20-Q3`+ onprem$`FY20-Q4`


training_onprem <- featurizeRev(onprem, "y18q3y19q2", "y19q3y20q2", "OnPremYoYChange", "OnPremMean", "OnPremYr1", "OnPremYr2", "201912") %>% select(FinalTPID, OnPremYoYChange, OnPremMean, OnPremYr1, OnPremYr2, ym)
scoring_onprem <- featurizeRev(onprem, "y19q1y19q4", "y20q1y20q4", "OnPremYoYChange", "OnPremMean", "OnPremYr1", "OnPremYr2", "202006") %>% select(FinalTPID, OnPremYoYChange, OnPremMean, OnPremYr1, OnPremYr2, ym)


#Other Rev
oq1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "18Q3") %>% filter(!is.na(FinalTPID))
oq2 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "18Q4") %>% filter(!is.na(FinalTPID))
oq3 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "19Q1") %>% filter(!is.na(FinalTPID))
oq4 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "19Q2") %>% filter(!is.na(FinalTPID))
oq5 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "19Q3") %>% filter(!is.na(FinalTPID))
oq6 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "19Q4") %>% filter(!is.na(FinalTPID))
oq7 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "20Q1") %>% filter(!is.na(FinalTPID))
oq8 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "20Q2") %>% filter(!is.na(FinalTPID))
oq9 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "20Q3") %>% filter(!is.na(FinalTPID))
oq10 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Revenue\\OtherRevenue.xlsx", sheet = "20Q4") %>% filter(!is.na(FinalTPID))

other_rev <- 
  full_join(oq1, oq2, by = "FinalTPID") %>%
  full_join(., oq3, by = "FinalTPID") %>%
  full_join(., oq4, by = "FinalTPID") %>%
  full_join(., oq5, by = "FinalTPID") %>%
  full_join(., oq6, by = "FinalTPID") %>% 
  full_join(., oq7, by = "FinalTPID") %>% 
  full_join(., oq8, by = "FinalTPID") %>% 
  full_join(., oq9, by = "FinalTPID") %>% 
  full_join(., oq10, by = "FinalTPID")

other_rev[is.na(other_rev)] <- 0

ProcessOtherRev <- function(data, colname, fy1, fy2, fy3, fy4)
{
  colname1 <- paste(colname, fy1, fy4, sep = '')
  #print (colname1)
  fy1 <- paste(colname, fy1, sep ='')
  fy2 <- paste(colname, fy2, sep ='')
  fy3 <- paste(colname, fy3, sep ='')
  fy4 <- paste(colname, fy4, sep ='')
  
  data[[colname1]] <- data[[fy1]] + data[[fy2]] + data[[fy3]] + data[[fy4]]
  return(data)
}

other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreM365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreM365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreNonM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreNonM365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreNonM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityCoreNonM365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityE5M365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityE5M365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityE5M365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityE5M365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityE5NonM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityE5NonM365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityE5NonM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "EnterpriseMobilityE5NonM365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")


other_rev <- ProcessOtherRev(other_rev, "O365CoreM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "O365CoreM365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "O365CoreM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "O365CoreM365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")


other_rev <- ProcessOtherRev(other_rev, "O365CoreNonM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "O365CoreNonM365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "O365CoreNonM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "O365CoreNonM365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")


other_rev <- ProcessOtherRev(other_rev, "O365E5M365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "O365E5M365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "O365E5M365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "O365E5M365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "O365E5NonM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "O365E5NonM365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "O365E5NonM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "O365E5NonM365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "O365E5SecurityAnalytics", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "O365E5SecurityAnalytics", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "O365E5SecurityAnalytics", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "O365E5SecurityAnalytics", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "Office", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "Office", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "Office", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "Office", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "PowerBIM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "PowerBIM365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "PowerBIM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "PowerBIM365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "PowerBINonM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "PowerBINonM365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "PowerBINonM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "PowerBINonM365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "WindowsCoreM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreM365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreM365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365E3", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365E3", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365E3", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365E3", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365Enterprise", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365Enterprise", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365Enterprise", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "WindowsCoreNonM365Enterprise", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "WindowsDeviceLicensing", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsDeviceLicensing", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsDeviceLicensing", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "WindowsDeviceLicensing", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "WindowsE5M365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsE5M365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsE5M365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "WindowsE5M365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")

other_rev <- ProcessOtherRev(other_rev, "WindowsE5NonM365", "FY18Q3", "FY18Q4", "FY19Q1", "FY19Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsE5NonM365", "FY19Q3", "FY19Q4", "FY20Q1", "FY20Q2")
other_rev <- ProcessOtherRev(other_rev, "WindowsE5NonM365", "FY19Q1", "FY19Q2", "FY19Q3", "FY19Q4")
other_rev <- ProcessOtherRev(other_rev, "WindowsE5NonM365", "FY20Q1", "FY20Q2", "FY20Q3", "FY20Q4")


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
training_cohort_1 <- featurize(other_rev, "EnterpriseMobilityCoreM365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, EnterpriseMobilityCoreM365YoYChange, EnterpriseMobilityCoreM365Mean, EnterpriseMobilityCoreM365Yr1, EnterpriseMobilityCoreM365Yr2, ym)
scoring_cohort_1 <- featurize(other_rev, "EnterpriseMobilityCoreM365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, EnterpriseMobilityCoreM365YoYChange, EnterpriseMobilityCoreM365Mean, EnterpriseMobilityCoreM365Yr1, EnterpriseMobilityCoreM365Yr2, ym)

training_cohort_2 <- featurize(other_rev, "EnterpriseMobilityCoreNonM365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, EnterpriseMobilityCoreNonM365YoYChange, EnterpriseMobilityCoreNonM365Mean, EnterpriseMobilityCoreNonM365Yr1, EnterpriseMobilityCoreNonM365Yr2, ym)
scoring_cohort_2 <- featurize(other_rev, "EnterpriseMobilityCoreNonM365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, EnterpriseMobilityCoreNonM365YoYChange, EnterpriseMobilityCoreNonM365Mean, EnterpriseMobilityCoreNonM365Yr1, EnterpriseMobilityCoreNonM365Yr2, ym)


training_cohort_3 <- featurize(other_rev, "EnterpriseMobilityE5M365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, EnterpriseMobilityE5M365YoYChange, EnterpriseMobilityE5M365Mean, EnterpriseMobilityE5M365Yr1, EnterpriseMobilityE5M365Yr2, ym)
scoring_cohort_3 <- featurize(other_rev, "EnterpriseMobilityE5M365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, EnterpriseMobilityE5M365YoYChange, EnterpriseMobilityE5M365Mean, EnterpriseMobilityE5M365Yr1, EnterpriseMobilityE5M365Yr2, ym)

training_cohort_4 <- featurize(other_rev, "EnterpriseMobilityE5NonM365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, EnterpriseMobilityE5NonM365YoYChange, EnterpriseMobilityE5NonM365Mean, EnterpriseMobilityE5NonM365Yr1, EnterpriseMobilityE5NonM365Yr2, ym)
scoring_cohort_4 <- featurize(other_rev, "EnterpriseMobilityE5NonM365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, EnterpriseMobilityE5NonM365YoYChange, EnterpriseMobilityE5NonM365Mean, EnterpriseMobilityE5NonM365Yr1, EnterpriseMobilityE5NonM365Yr2, ym)

training_cohort_5 <- featurize(other_rev, "O365CoreM365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, O365CoreM365YoYChange, O365CoreM365Mean, O365CoreM365Yr1, O365CoreM365Yr2, ym)
scoring_cohort_5 <- featurize(other_rev, "O365CoreM365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, O365CoreM365YoYChange, O365CoreM365Mean, O365CoreM365Yr1, O365CoreM365Yr2, ym)

training_cohort_6 <- featurize(other_rev, "O365CoreNonM365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, O365CoreNonM365YoYChange, O365CoreNonM365Mean, O365CoreNonM365Yr1, O365CoreNonM365Yr2, ym)
scoring_cohort_6 <- featurize(other_rev, "O365CoreNonM365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, O365CoreNonM365YoYChange, O365CoreNonM365Mean, O365CoreNonM365Yr1, O365CoreNonM365Yr2, ym)

training_cohort_7 <- featurize(other_rev, "O365E5M365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, O365E5M365YoYChange, O365E5M365Mean, O365E5M365Yr1, O365E5M365Yr2, ym)
scoring_cohort_7 <- featurize(other_rev, "O365E5M365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, O365E5M365YoYChange, O365E5M365Mean, O365E5M365Yr1, O365E5M365Yr2, ym)

training_cohort_8 <- featurize(other_rev, "O365E5NonM365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, O365E5NonM365YoYChange, O365E5NonM365Mean, O365E5NonM365Yr1, O365E5NonM365Yr2, ym)
scoring_cohort_8 <- featurize(other_rev, "O365E5NonM365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, O365E5NonM365YoYChange, O365E5NonM365Mean, O365E5NonM365Yr1, O365E5NonM365Yr2, ym)

training_cohort_9 <- featurize(other_rev, "O365E5SecurityAnalytics", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, O365E5SecurityAnalyticsYoYChange, O365E5SecurityAnalyticsMean, O365E5SecurityAnalyticsYr1, O365E5SecurityAnalyticsYr2, ym)
scoring_cohort_9 <- featurize(other_rev, "O365E5SecurityAnalytics", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, O365E5SecurityAnalyticsYoYChange, O365E5SecurityAnalyticsMean, O365E5SecurityAnalyticsYr1, O365E5SecurityAnalyticsYr2, ym)

training_cohort_10 <- featurize(other_rev, "Office", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, OfficeYoYChange, OfficeMean, OfficeYr1, OfficeYr2, ym)
scoring_cohort_10 <- featurize(other_rev, "Office", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, OfficeYoYChange, OfficeMean, OfficeYr1, OfficeYr2, ym)

training_cohort_11 <- featurize(other_rev, "PowerBIM365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, PowerBIM365YoYChange, PowerBIM365Mean, PowerBIM365Yr1, PowerBIM365Yr2, ym)
scoring_cohort_11 <- featurize(other_rev, "PowerBIM365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, PowerBIM365YoYChange, PowerBIM365Mean, PowerBIM365Yr1, PowerBIM365Yr2, ym)

training_cohort_12 <- featurize(other_rev, "PowerBINonM365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, PowerBINonM365YoYChange, PowerBINonM365Mean, PowerBINonM365Yr1, PowerBINonM365Yr2, ym)
scoring_cohort_12 <- featurize(other_rev, "PowerBINonM365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, PowerBINonM365YoYChange, PowerBINonM365Mean, PowerBINonM365Yr1, PowerBINonM365Yr2, ym)

training_cohort_13 <- featurize(other_rev, "WindowsCoreM365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, WindowsCoreM365YoYChange, WindowsCoreM365Mean, WindowsCoreM365Yr1, WindowsCoreM365Yr2, ym)
scoring_cohort_13 <- featurize(other_rev, "WindowsCoreM365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, WindowsCoreM365YoYChange, WindowsCoreM365Mean, WindowsCoreM365Yr1, WindowsCoreM365Yr2, ym)

training_cohort_14 <- featurize(other_rev, "WindowsCoreNonM365E3", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, WindowsCoreNonM365E3YoYChange, WindowsCoreNonM365E3Mean, WindowsCoreNonM365E3Yr1, WindowsCoreNonM365E3Yr2, ym)
scoring_cohort_14 <- featurize(other_rev, "WindowsCoreNonM365E3", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, WindowsCoreNonM365E3YoYChange, WindowsCoreNonM365E3Mean, WindowsCoreNonM365E3Yr1, WindowsCoreNonM365E3Yr2, ym)


training_cohort_18 <- featurize(other_rev, "WindowsCoreNonM365Enterprise", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, WindowsCoreNonM365EnterpriseYoYChange, WindowsCoreNonM365EnterpriseMean, WindowsCoreNonM365EnterpriseYr1, WindowsCoreNonM365EnterpriseYr2, ym)
scoring_cohort_18 <- featurize(other_rev, "WindowsCoreNonM365Enterprise", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, WindowsCoreNonM365EnterpriseYoYChange, WindowsCoreNonM365EnterpriseMean, WindowsCoreNonM365EnterpriseYr1, WindowsCoreNonM365EnterpriseYr2, ym)

training_cohort_15 <- featurize(other_rev, "WindowsDeviceLicensing", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, WindowsDeviceLicensingYoYChange, WindowsDeviceLicensingMean, WindowsDeviceLicensingYr1, WindowsDeviceLicensingYr2, ym)
scoring_cohort_15 <- featurize(other_rev, "WindowsDeviceLicensing", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, WindowsDeviceLicensingYoYChange, WindowsDeviceLicensingMean, WindowsDeviceLicensingYr1, WindowsDeviceLicensingYr2, ym)

training_cohort_16 <- featurize(other_rev, "WindowsE5M365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, WindowsE5M365YoYChange, WindowsE5M365Mean, WindowsE5M365Yr1, WindowsE5M365Yr2, ym)
scoring_cohort_16 <- featurize(other_rev, "WindowsE5M365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, WindowsE5M365YoYChange, WindowsE5M365Mean, WindowsE5M365Yr1, WindowsE5M365Yr2, ym)

training_cohort_17 <- featurize(other_rev, "WindowsE5NonM365", "FY18Q3FY19Q2", "FY19Q3FY20Q2", "YoYChange", "Mean", "Yr1", "Yr2", "201912") %>% select(FinalTPID, WindowsE5NonM365YoYChange, WindowsE5NonM365Mean, WindowsE5NonM365Yr1, WindowsE5NonM365Yr2, ym)
scoring_cohort_17 <- featurize(other_rev, "WindowsE5NonM365", "FY19Q1FY19Q4", "FY20Q1FY20Q4", "YoYChange", "Mean", "Yr1", "Yr2", "202006") %>% select(FinalTPID, WindowsE5NonM365YoYChange, WindowsE5NonM365Mean, WindowsE5NonM365Yr1, WindowsE5NonM365Yr2, ym)


revenue_training_data <- full_join(training_cohort_1, training_cohort_2, by = c("FinalTPID", "ym")) %>% 
  full_join(., training_cohort_3, by = c("FinalTPID", "ym")) %>% 
  full_join(., training_cohort_4, by = c("FinalTPID", "ym")) %>% 
  full_join(., training_cohort_5, by = c("FinalTPID", "ym")) %>% 
  full_join(., training_cohort_6, by = c("FinalTPID", "ym")) %>% 
  full_join(., training_cohort_7, by = c("FinalTPID", "ym")) %>% 
  full_join(., training_cohort_8, by = c("FinalTPID", "ym")) %>% 
full_join(., training_cohort_9, by = c("FinalTPID", "ym")) %>% 
full_join(., training_cohort_10, by = c("FinalTPID", "ym")) %>% 
full_join(., training_cohort_11, by = c("FinalTPID", "ym")) %>% 
full_join(., training_cohort_12, by = c("FinalTPID", "ym")) %>% 
full_join(., training_cohort_13, by = c("FinalTPID", "ym")) %>% 
full_join(., training_cohort_14, by = c("FinalTPID", "ym")) %>% 
full_join(., training_cohort_15, by = c("FinalTPID", "ym")) %>% 
full_join(., training_cohort_16, by = c("FinalTPID", "ym")) %>% 
full_join(., training_cohort_17, by = c("FinalTPID", "ym")) %>% 
  full_join(., training_cohort_18, by = c("FinalTPID", "ym"))

revenue_scoring_data <- full_join(scoring_cohort_1, scoring_cohort_2, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_3, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_4, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_5, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_6, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_7, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_8, by = c("FinalTPID", "ym")) %>% 
full_join(., scoring_cohort_9, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_10, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_11, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_12, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_13, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_14, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_15, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_16, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_17, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_cohort_18, by = c("FinalTPID", "ym")) 


training_revenue <- 
  full_join(training_azure, training_dynamics, by = c("FinalTPID", "ym")) %>% 
  full_join(., training_onprem,by = c("FinalTPID", "ym")) %>% 
  full_join(., revenue_training_data, by = c("FinalTPID", "ym"))

write.csv(training_revenue, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Revenue\\Training_Revenue.csv", row.names = FALSE)

scoring_revenue <- 
  full_join(scoring_azure, scoring_dynamics, by = c("FinalTPID", "ym")) %>% 
  full_join(., scoring_onprem,by = c("FinalTPID", "ym")) %>% 
  full_join(., revenue_scoring_data, by = c("FinalTPID", "ym"))

write.csv(scoring_revenue, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Revenue\\Scoring_Revenue.csv", row.names = FALSE)
