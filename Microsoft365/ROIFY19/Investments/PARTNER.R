# Script to process Partner data # 
library(readxl)
library(zoo)
require(CosmosToR)
require(dplyr)
library(tidyr)
library(lubridate)


# Need to calculate # of months a payment was made
# Need to calculate 3 different files based on the workloads involved. 

###############################################
# for filtering for specific workloads  
all_wkl <- c("ProPlus", "Teams","SPO", "EMS - AADP", "EMS - Intune", "AADP", "Microsoft Intune", "exo", "spo", "teams", "intune", "proplus", "aadp") # OLM not there here 
all_wkl <- data.frame(all_wkl)
colnames(all_wkl) <- "WorkloadName"

core_wkl <- c("ProPlus", "SPO", "EMS - AADP", "EMS - Intune", "AADP", "Microsoft Intune", "exo", "spo", "intune", "proplus", "aadp")
core_wkl <- data.frame(core_wkl)
colnames(core_wkl) <- "WorkloadName"

modern_comms <- c("Teams", "teams")
modern_comms <- data.frame(modern_comms)
colnames(modern_comms) <- "WorkloadName"

###############################################

# Input reading
partner_data <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\CPOREventsFull.xlsx") 
partner_data$TenantId <- tolower(partner_data$TenantId)
partner_data <- partner_data %>% select(TenantId, MonthEndEventDate, WorkloadName, EventTitle)

calculateMonths <- function(data, start_date, end_date, var1)
{
  
  # No of months since first payment, # of payments made in the time period
  data1 <- data %>% filter(MonthEndEventDate >= start_date & MonthEndEventDate <= end_date)
  
  #data11 <- data1 %>% group_by(TenantId) %>% summarise(NoOfPayments = n())
  data1 <- data1 %>% group_by(TenantId, MonthEndEventDate) %>% slice(1)
  data1 <- data1 %>% group_by(TenantId) %>% summarise(NoOfMonths= n())
  data1[var1] <- data1$NoOfMonths
  data1 <- data1[,c(1,3)]
  
  return (data1)
}

# Each time for AllWkl, CoreWkl, Modern Comms
all_wkl <- inner_join(partner_data, all_wkl, by = "WorkloadName")
jul1819 <- calculateMonths(all_wkl, "2018-08-01", "2019-05-31", "PartnerNoOfPaymentMonthsJul")
aug1819 <- calculateMonths(all_wkl, "2018-09-01", "2019-06-30", "PartnerNoOfPaymentMonthsAug")
sep1819 <- calculateMonths(all_wkl, "2018-10-01", "2019-07-31", "PartnerNoOfPaymentMonthsSep")
oct1819 <- calculateMonths(all_wkl, "2018-11-01", "2019-08-31", "PartnerNoOfPaymentMonthsOct")
nov1819 <- calculateMonths(all_wkl, "2018-12-01", "2019-09-30", "PartnerNoOfPaymentMonthsNov")
dec1819 <- calculateMonths(all_wkl, "2019-01-01", "2019-10-31", "PartnerNoOfPaymentMonthsDec")

all_wkl <- full_join(jul1819, aug1819, by = "TenantId") %>% 
  full_join(., sep1819, by = "TenantId") %>% 
  full_join(., oct1819, by = "TenantId") %>% 
  full_join(., nov1819, by = "TenantId") %>% 
  full_join(., dec1819, by = "TenantId")

write.csv(all_wkl, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\Partner_AllWkl.csv", row.names = FALSE)  

# Each time for AllWkl, CoreWkl, Modern Comms
core_wkl <- inner_join(partner_data, core_wkl, by = "WorkloadName")
jul1819 <- calculateMonths(core_wkl, "2018-08-01", "2019-05-31", "PartnerNoOfPaymentMonthsJul")
aug1819 <- calculateMonths(core_wkl, "2018-09-01", "2019-06-30", "PartnerNoOfPaymentMonthsAug")
sep1819 <- calculateMonths(core_wkl, "2018-10-01", "2019-07-31", "PartnerNoOfPaymentMonthsSep")
oct1819 <- calculateMonths(core_wkl, "2018-11-01", "2019-08-31", "PartnerNoOfPaymentMonthsOct")
nov1819 <- calculateMonths(core_wkl, "2018-12-01", "2019-09-30", "PartnerNoOfPaymentMonthsNov")
dec1819 <- calculateMonths(core_wkl, "2019-01-01", "2019-10-31", "PartnerNoOfPaymentMonthsDec")
core_wkl <- full_join(jul1819, aug1819, by = "TenantId") %>% 
  full_join(., sep1819, by = "TenantId") %>% 
  full_join(., oct1819, by = "TenantId") %>% 
  full_join(., nov1819, by = "TenantId") %>% 
  full_join(., dec1819, by = "TenantId")

write.csv(core_wkl, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CoreWorkloads\\Partner_CoreWkl.csv", row.names = FALSE)  

# Each time for AllWkl, CoreWkl, Modern Comms
modern_comms <- inner_join(partner_data, modern_comms, by = "WorkloadName")
jul1819 <- calculateMonths(modern_comms, "2018-08-01", "2019-05-31", "PartnerNoOfPaymentMonthsJul")
aug1819 <- calculateMonths(modern_comms, "2018-09-01", "2019-06-30", "PartnerNoOfPaymentMonthsAug")
sep1819 <- calculateMonths(modern_comms, "2018-10-01", "2019-07-31", "PartnerNoOfPaymentMonthsSep")
oct1819 <- calculateMonths(modern_comms, "2018-11-01", "2019-08-31", "PartnerNoOfPaymentMonthsOct")
nov1819 <- calculateMonths(modern_comms, "2018-12-01", "2019-09-30", "PartnerNoOfPaymentMonthsNov")
dec1819 <- calculateMonths(modern_comms, "2019-01-01", "2019-10-31", "PartnerNoOfPaymentMonthsDec")

modern_comms <- full_join(jul1819, aug1819, by = "TenantId") %>% 
  full_join(., sep1819, by = "TenantId") %>% 
  full_join(., oct1819, by = "TenantId") %>% 
  full_join(., nov1819, by = "TenantId") %>% 
  full_join(., dec1819, by = "TenantId")

write.csv(modern_comms, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\Partner_ModernComms.csv", row.names = FALSE)  