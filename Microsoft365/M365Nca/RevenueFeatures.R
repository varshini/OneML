library(dplyr)
library(readxl)
library(tidyr)
library(data.table)
require(TTR)
require(quantmod)
require(caTools)
options(stringsAsFactors = FALSE)

# Onprem revenue - Mean, YOY growth, actual yr1, actual yr2
onprem <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\OnPrem.xlsx")
onprem[is.na(onprem)] <- 0

onprem$y17q2y18q1 <- onprem$`FY17-Q2`+ onprem$`FY17-Q3`+ onprem$`FY17-Q4` + onprem$`FY18-Q1`
onprem$y18q2y19q1 <- onprem$`FY18-Q2` + onprem$`FY18-Q3`+ onprem$`FY18-Q4`+ onprem$`FY19-Q1`

onprem$y17q3y18q2 <- onprem$`FY17-Q3`+ onprem$`FY17-Q4`+ onprem$`FY18-Q1` + onprem$`FY18-Q2`
onprem$y18q3y19q2 <- onprem$`FY18-Q3` + onprem$`FY18-Q4`+ onprem$`FY19-Q1`+ onprem$`FY19-Q2`

onprem$y17q4y18q3 <- onprem$`FY17-Q4`+ onprem$`FY18-Q1`+ onprem$`FY18-Q2` + onprem$`FY18-Q3`
onprem$y18q4y19q3 <- onprem$`FY18-Q4` + onprem$`FY19-Q1`+ onprem$`FY19-Q2`+ onprem$`FY19-Q3`

onprem$y18q1y18q4 <- onprem$`FY18-Q1`+ onprem$`FY18-Q2`+ onprem$`FY18-Q3` + onprem$`FY18-Q4`
onprem$y19q1y19q4 <- onprem$`FY19-Q1` + onprem$`FY19-Q2`+ onprem$`FY19-Q3`+ onprem$`FY19-Q4`

featurizeOnPrem <- function (data, yr1, yr2, outputCol1, outputCol2, outputCol3, outputCol4, ym)
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

training_cohort_q3_onprem <- featurizeOnPrem(onprem, "y17q2y18q1", "y18q2y19q1", "OnPremYoYChange", "OnPremMean", "OnPremYr1", "OnPremYr2", "FY19Q3") %>% select(TPID, OnPremYoYChange, OnPremMean, OnPremYr1, OnPremYr2, ym)
training_cohort_q4_onprem <- featurizeOnPrem(onprem, "y17q3y18q2", "y18q3y19q2", "OnPremYoYChange", "OnPremMean", "OnPremYr1", "OnPremYr2", "FY19Q4") %>% select(TPID, OnPremYoYChange, OnPremMean, OnPremYr1, OnPremYr2, ym)
training_cohort_onprem <- rbind(training_cohort_q3_onprem, training_cohort_q4_onprem)

scoring_cohort_q1_onprem <- featurizeOnPrem(onprem, "y17q4y18q3", "y18q4y19q3", "OnPremYoYChange", "OnPremMean", "OnPremYr1", "OnPremYr2", "FY20Q1") %>% select(TPID, OnPremYoYChange, OnPremMean, OnPremYr1, OnPremYr2, ym)
scoring_cohort_q2_onprem <- featurizeOnPrem(onprem, "y18q1y18q4", "y19q1y19q4", "OnPremYoYChange", "OnPremMean", "OnPremYr1", "OnPremYr2", "FY20Q2") %>% select(TPID, OnPremYoYChange, OnPremMean, OnPremYr1, OnPremYr2, ym)
scoring_cohort_onprem <- rbind(scoring_cohort_q1_onprem, scoring_cohort_q2_onprem)

# Azure Revenue 

azure <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\AzureACR.xlsx", sheet = "ACRProc")
azure[is.na(azure)] <- 0

azure$y17q2y18q1 <- azure$`FY17-Q2`+ azure$`FY17-Q3`+ azure$`FY17-Q4` + azure$`FY18-Q1`
azure$y18q2y19q1 <- azure$`FY18-Q2` + azure$`FY18-Q3`+ azure$`FY18-Q4`+ azure$`FY19-Q1`

azure$y17q3y18q2 <- azure$`FY17-Q3`+ azure$`FY17-Q4`+ azure$`FY18-Q1` + azure$`FY18-Q2`
azure$y18q3y19q2 <- azure$`FY18-Q3` + azure$`FY18-Q4`+ azure$`FY19-Q1`+ azure$`FY19-Q2`

azure$y17q4y18q3 <- azure$`FY17-Q4`+ azure$`FY18-Q1`+ azure$`FY18-Q2` + azure$`FY18-Q3`
azure$y18q4y19q3 <- azure$`FY18-Q4` + azure$`FY19-Q1`+ azure$`FY19-Q2`+ azure$`FY19-Q3`

azure$y18q1y18q4 <- azure$`FY18-Q1`+ azure$`FY18-Q2`+ azure$`FY18-Q3` + azure$`FY18-Q4`
azure$y19q1y19q4 <- azure$`FY19-Q1` + azure$`FY19-Q2`+ azure$`FY19-Q3`+ azure$`FY19-Q4`

training_cohort_q3_azure <- featurizeOnPrem(azure, "y17q2y18q1", "y18q2y19q1", "AzureYoYChange", "AzureMean", "AzureYr1", "AzureYr2", "FY19Q3") %>% select(TPID, AzureYoYChange, AzureMean, AzureYr1, AzureYr2, ym)
training_cohort_q4_azure <- featurizeOnPrem(azure, "y17q3y18q2", "y18q3y19q2", "AzureYoYChange", "AzureMean", "AzureYr1", "AzureYr2", "FY19Q4") %>% select(TPID, AzureYoYChange, AzureMean, AzureYr1, AzureYr2, ym)
training_cohort_azure <- rbind(training_cohort_q3_azure, training_cohort_q4_azure)

scoring_cohort_q1_azure <- featurizeOnPrem(azure, "y17q4y18q3", "y18q4y19q3", "AzureYoYChange", "AzureMean", "AzureYr1", "AzureYr2", "FY20Q1") %>% select(TPID, AzureYoYChange, AzureMean, AzureYr1, AzureYr2, ym)
scoring_cohort_q2_azure <- featurizeOnPrem(azure, "y18q1y18q4", "y19q1y19q4", "AzureYoYChange", "AzureMean", "AzureYr1", "AzureYr2", "FY20Q2") %>% select(TPID, AzureYoYChange, AzureMean, AzureYr1, AzureYr2, ym)
scoring_cohort_azure <- rbind(scoring_cohort_q1_azure, scoring_cohort_q2_azure)

# Dynamics Revenue 

dynamics <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Revenue\\DynamicsRevenue.xlsx")
dynamics[is.na(dynamics)] <- 0

dynamics$y17q2y18q1 <- dynamics$`FY17-Q2`+ dynamics$`FY17-Q3`+ dynamics$`FY17-Q4` + dynamics$`FY18-Q1`
dynamics$y18q2y19q1 <- dynamics$`FY18-Q2` + dynamics$`FY18-Q3`+ dynamics$`FY18-Q4`+ dynamics$`FY19-Q1`

dynamics$y17q3y18q2 <- dynamics$`FY17-Q3`+ dynamics$`FY17-Q4`+ dynamics$`FY18-Q1` + dynamics$`FY18-Q2`
dynamics$y18q3y19q2 <- dynamics$`FY18-Q3` + dynamics$`FY18-Q4`+ dynamics$`FY19-Q1`+ dynamics$`FY19-Q2`

dynamics$y17q4y18q3 <- dynamics$`FY17-Q4`+ dynamics$`FY18-Q1`+ dynamics$`FY18-Q2` + dynamics$`FY18-Q3`
dynamics$y18q4y19q3 <- dynamics$`FY18-Q4` + dynamics$`FY19-Q1`+ dynamics$`FY19-Q2`+ dynamics$`FY19-Q3`

dynamics$y18q1y18q4 <- dynamics$`FY18-Q1`+ dynamics$`FY18-Q2`+ dynamics$`FY18-Q3` + dynamics$`FY18-Q4`
dynamics$y19q1y19q4 <- dynamics$`FY19-Q1` + dynamics$`FY19-Q2`+ dynamics$`FY19-Q3`+ dynamics$`FY19-Q4`

training_cohort_q3_dynamics <- featurizeOnPrem(dynamics, "y17q2y18q1", "y18q2y19q1", "dynamicsYoYChange", "dynamicsMean", "dynamicsYr1", "dynamicsYr2", "FY19Q3") %>% select(TPID, dynamicsYoYChange, dynamicsMean, dynamicsYr1, dynamicsYr2, ym)
training_cohort_q4_dynamics <- featurizeOnPrem(dynamics, "y17q3y18q2", "y18q3y19q2", "dynamicsYoYChange", "dynamicsMean", "dynamicsYr1", "dynamicsYr2", "FY19Q4") %>% select(TPID, dynamicsYoYChange, dynamicsMean, dynamicsYr1, dynamicsYr2, ym)
training_cohort_dynamics <- rbind(training_cohort_q3_dynamics, training_cohort_q4_dynamics)

scoring_cohort_q1_dynamics <- featurizeOnPrem(dynamics, "y17q4y18q3", "y18q4y19q3", "dynamicsYoYChange", "dynamicsMean", "dynamicsYr1", "dynamicsYr2", "FY20Q1") %>% select(TPID, dynamicsYoYChange, dynamicsMean, dynamicsYr1, dynamicsYr2, ym)
scoring_cohort_q2_dynamics <- featurizeOnPrem(dynamics, "y18q1y18q4", "y19q1y19q4", "dynamicsYoYChange", "dynamicsMean", "dynamicsYr1", "dynamicsYr2", "FY20Q2") %>% select(TPID, dynamicsYoYChange, dynamicsMean, dynamicsYr1, dynamicsYr2, ym)
scoring_cohort_dynamics <- rbind(scoring_cohort_q1_dynamics, scoring_cohort_q2_dynamics)


# Other Revenue - O365, EMS, Windows - Load the training and scoring file
training_cohort_other <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\OtherRevenue.csv")
scoring_cohort_other <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\OtherRevenue.csv")

# full join all the data and make it one revenue file #
training_cohort_onprem$TPID <- as.numeric(training_cohort_onprem$TPID)
training_cohort_other$TPID <- as.numeric(training_cohort_other$TPID)
training_cohort_azure$TPID <- as.numeric(training_cohort_azure$TPID)
training_cohort_dynamics$TPID <- as.numeric(training_cohort_dynamics$TPID)

scoring_cohort_onprem$TPID <- as.numeric(scoring_cohort_onprem$TPID)
scoring_cohort_other$TPID <- as.numeric(scoring_cohort_other$TPID)
scoring_cohort_azure$TPID <- as.numeric(scoring_cohort_azure$TPID)
scoring_cohort_dynamics$TPID <- as.numeric(scoring_cohort_dynamics$TPID)

final_license_training <- full_join(training_cohort_onprem, training_cohort_other, by = c("TPID", "ym")) %>% 
  full_join(., training_cohort_azure, by = c("TPID", "ym")) %>% 
  full_join(., training_cohort_dynamics, by = c("TPID", "ym")) 
final_license_training[is.na(final_license_training)] <- 0

final_license_scoring <- full_join(scoring_cohort_onprem, scoring_cohort_other, by = c("TPID", "ym")) %>% 
  full_join(., scoring_cohort_azure, by = c("TPID", "ym")) %>% 
  full_join(., scoring_cohort_dynamics, by = c("TPID", "ym")) 
final_license_scoring[is.na(final_license_scoring)] <- 0

write.csv(final_license_training, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\Revenue.csv", row.names = FALSE)
write.csv(final_license_scoring, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\Revenue.csv", row.names = FALSE)


# Geo and Industry
geo_indus <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\GeoIndustry.xlsx")

geo_indus$GeoMapping <- as.numeric(as.factor(geo_indus$Area))
geo_indus$IndustryMapping <- as.numeric(as.factor(geo_indus$Industry))


write.csv(geo_indus, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\GeoIndustry.csv", row.names = FALSE)
write.csv(geo_indus, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\GeoIndustry.csv", row.names = FALSE)




