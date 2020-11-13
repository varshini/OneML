library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
options(stringsAsFactors = FALSE)

tenant_events <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\TenantEvents.csv")
tenant_events_temp <- tenant_events
tenant_events_temp$MonthEndEventDate <- as.Date(mdy_hms(tenant_events_temp$MonthEndEventDate))

###############################################
# for filtering for specific workloads  
all_wkl <- c("OfficeProPlus", "Teams","SharePoint", "Intune", "AADP", "Exchange", "OneDrive") 
all_wkl <- data.frame(all_wkl)
colnames(all_wkl) <- "ServiceNameEvent"

core_wkl <- c("OfficeProPlus","SharePoint", "Intune", "AADP", "Exchange", "OneDrive") 
core_wkl <- data.frame(core_wkl)
colnames(core_wkl) <- "ServiceNameEvent"

modern_comms <- c("Teams")
modern_comms <- data.frame(modern_comms)
colnames(modern_comms) <- "ServiceNameEvent"

###############################################

frp_payments <- filter(tenant_events_temp, EventCategory == "FRP Incentive Payments" & MonthEndEventDate)
frp_payments <- frp_payments %>% select(TenantId, ServiceNameEvent, EventTitle, MonthEndEventDate)

calculateMonths <- function(data, start_date, end_date, var1)
{
# No of months since first payment, # of payments made in the time period
data1 <- data %>% filter(MonthEndEventDate >= start_date & MonthEndEventDate <= end_date)
data1 <- data1 %>% group_by(TenantId, MonthEndEventDate) %>% slice(1)
data1 <- data1 %>% group_by(TenantId) %>% summarise(NoOfMonths= n())
data1[var1] <- data1$NoOfMonths
data1 <- data1[,c(1,3)]
return (data1)
}

# Each time for AllWkl, CoreWkl, Modern Comms
all_wkl <- inner_join(frp_payments, all_wkl, by = "ServiceNameEvent")
jul1819 <- calculateMonths(all_wkl, "2018-08-01", "2019-05-31", "FRPNoOfPaymentMonthsJul")
aug1819 <- calculateMonths(all_wkl, "2018-09-01", "2019-06-30", "FRPNoOfPaymentMonthsAug")
sep1819 <- calculateMonths(all_wkl, "2018-10-01", "2019-07-31", "FRPNoOfPaymentMonthsSep")
oct1819 <- calculateMonths(all_wkl, "2018-11-01", "2019-08-31", "FRPNoOfPaymentMonthsOct")
nov1819 <- calculateMonths(all_wkl, "2018-12-01", "2019-09-30", "FRPNoOfPaymentMonthsNov")
dec1819 <- calculateMonths(all_wkl, "2019-01-01", "2019-10-31", "FRPNoOfPaymentMonthsDec")

all_wkl <- full_join(jul1819, aug1819, by = "TenantId") %>% 
  full_join(., sep1819, by = "TenantId") %>% 
  full_join(., oct1819, by = "TenantId") %>% 
  full_join(., nov1819, by = "TenantId") %>% 
  full_join(., dec1819, by = "TenantId")

write.csv(all_wkl, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FRP_AllWkl.csv", row.names = FALSE)  


core_wkl <- inner_join(frp_payments, core_wkl, by = "ServiceNameEvent")
jul1819 <- calculateMonths(core_wkl, "2018-08-01", "2019-05-31", "FRPNoOfPaymentMonthsJul")
aug1819 <- calculateMonths(core_wkl, "2018-09-01", "2019-06-30", "FRPNoOfPaymentMonthsAug")
sep1819 <- calculateMonths(core_wkl, "2018-10-01", "2019-07-31", "FRPNoOfPaymentMonthsSep")
oct1819 <- calculateMonths(core_wkl, "2018-11-01", "2019-08-31", "FRPNoOfPaymentMonthsOct")
nov1819 <- calculateMonths(core_wkl, "2018-12-01", "2019-09-30", "FRPNoOfPaymentMonthsNov")
dec1819 <- calculateMonths(core_wkl, "2019-01-01", "2019-10-31", "FRPNoOfPaymentMonthsDec")

core_wkl <- full_join(jul1819, aug1819, by = "TenantId") %>% 
  full_join(., sep1819, by = "TenantId") %>% 
  full_join(., oct1819, by = "TenantId") %>% 
  full_join(., nov1819, by = "TenantId") %>% 
  full_join(., dec1819, by = "TenantId")

write.csv(core_wkl, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CoreWorkloads\\FRP_CoreWkl.csv", row.names = FALSE)  

modern_comms <- inner_join(frp_payments, modern_comms, by = "ServiceNameEvent")
jul1819 <- calculateMonths(modern_comms, "2018-08-01", "2019-05-31", "FRPNoOfPaymentMonthsJul")
aug1819 <- calculateMonths(modern_comms, "2018-09-01", "2019-06-30", "FRPNoOfPaymentMonthsAug")
sep1819 <- calculateMonths(modern_comms, "2018-10-01", "2019-07-31", "FRPNoOfPaymentMonthsSep")
oct1819 <- calculateMonths(modern_comms, "2018-11-01", "2019-08-31", "FRPNoOfPaymentMonthsOct")
nov1819 <- calculateMonths(modern_comms, "2018-12-01", "2019-09-30", "FRPNoOfPaymentMonthsNov")
dec1819 <- calculateMonths(modern_comms, "2019-01-01", "2019-10-31", "FRPNoOfPaymentMonthsDec")

modern_comms <- full_join(jul1819, aug1819, by = "TenantId") %>% 
  full_join(., sep1819, by = "TenantId") %>% 
  full_join(., oct1819, by = "TenantId") %>% 
  full_join(., nov1819, by = "TenantId") %>% 
  full_join(., dec1819, by = "TenantId")

write.csv(modern_comms, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\FRP_ModernComms.csv", row.names = FALSE) 