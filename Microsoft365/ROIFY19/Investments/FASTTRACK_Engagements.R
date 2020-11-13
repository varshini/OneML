# Refresh tenant events file
# Take shortlisted events, get the dates 
# calculate # of months of touch, creation date, and # of events that happened 

library(dplyr)
library(tidyr)
library(lubridate)
options(stringsAsFactors = FALSE)


#tenant events from Cosmos stream
tenant_events <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\TenantEvents.csv")
tenant_events_temp <- tenant_events
tenant_events_temp$MonthEndEventDate <- as.Date(mdy_hms(tenant_events_temp$MonthEndEventDate))

###############################################
# for filtering for specific workloads 
all_wkl <- c("Tenant Level","Exchange", "SPO", "OPP", "ODB", "Teams", "AADP", "Intune", "OfficeProPlus", "Exchange: Outlook Mobile","SharePoint","OneDrive","CSMAU","Any Service",
             "Office 365") 
all_wkl <- data.frame(all_wkl)
colnames(all_wkl) <- "ServiceNameEvent"

core_wkl <- c("Tenant Level","Exchange", "SPO", "OPP", "ODB", "OfficeProPlus", "Exchange: Outlook Mobile","SharePoint","OneDrive","CSMAU","Any Service",
              "Office 365", "AADP", "Intune")
core_wkl <- data.frame(core_wkl)
colnames(core_wkl) <- "ServiceNameEvent"

modern_comms <- c("Tenant Level","CSMAU","Any Service", "Office 365", "Teams")
modern_comms <- data.frame(modern_comms)
colnames(modern_comms) <- "ServiceNameEvent"

#lookup table for quarterly events to convert to monthly events
MonthEndEventDate <- c("2018-07-31", "2018-07-31", "2018-07-31",
                       "2018-10-31", "2018-10-31", "2018-10-31", 
                       "2019-01-31", "2019-01-31", "2019-01-31", 
                       "2019-04-30", "2019-04-30", "2019-04-30", 
                       "2019-07-31", "2019-07-31", "2019-07-31", 
                       "2019-10-31", "2019-10-31", "2019-10-31")

MonthDate <-   c("2018-07-31", "2018-08-31", "2018-09-30",
                 "2018-10-31", "2018-11-30", "2018-12-31", 
                 "2019-01-31", "2019-02-28", "2019-03-31", 
                 "2019-04-30", "2019-05-31", "2019-06-30", 
                 "2019-07-31", "2019-08-31", "2019-09-30", 
                 "2019-10-31", "2019-11-30", "2019-12-31")
lookup_table <- data.frame(MonthEndEventDate, MonthDate)
lookup_table$MonthEndEventDate <- as.Date(lookup_table$MonthEndEventDate)

###############################################

# FT engagement level features - Automation, People Assigned, FRP Assigned (Feature - # of months of touch in any of the involved workloads)
#CSS Engaged this month 

calculateMonths <- function(data, start_date, end_date, var1, var2, var3)
{

  data1 <- data %>% filter(EventTitle == "FT Delivery Channel-CSS Engaged this Month" & MonthEndEventDate >= start_date & MonthEndEventDate <= end_date)
  data1 <- data1 %>% group_by(TenantId, MonthEndEventDate) %>%  slice(1)
  data1 <- data1 %>% group_by(TenantId) %>% summarise(NoOfMonths = n())
  data1[var1] <- data1$NoOfMonths
  data1 <- data1[,c(1,3)]
  
  data2 <- data %>% filter(EventTitle == "FT Delivery Channel-FRP Engaged this Month" & MonthEndEventDate >= start_date & MonthEndEventDate <= end_date)
  data2 <- data2 %>% group_by(TenantId, MonthEndEventDate) %>%  slice(1)
  data2 <- data2 %>% group_by(TenantId) %>% summarise(NoOfMonths = n())
  data2[var2] <- data2$NoOfMonths
  data2 <- data2[,c(1,3)]
  
  data3 <- data %>% filter(EventTitle == "FT Automation Engaged this Quarter")
  data3 <- inner_join(data3, lookup_table, by = "MonthEndEventDate")
  data3 <- data3 %>% filter(MonthDate >= start_date & MonthDate <= end_date)
  data3 <- data3 %>% group_by(TenantId, MonthDate) %>%  slice(1)
  data3 <- data3 %>% group_by(TenantId) %>% summarise(NoOfMonths = n())
  data3[var3] <- data3$NoOfMonths
  data3 <- data3[,c(1,3)]
  
  final_data <- full_join(data1, data2, by = "TenantId") %>% 
  full_join(., data3, by = "TenantId")
  
  return (final_data)
}


# Each time for AllWkl, CoreWkl, Modern Comms
all_wkl <- inner_join(tenant_events_temp, all_wkl, by = "ServiceNameEvent")
jul1819 <- calculateMonths(all_wkl, "2018-08-01", "2019-05-31", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
jul1819$ym <- "201907"
aug1819 <- calculateMonths(all_wkl, "2018-09-01", "2019-06-30", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
aug1819$ym <- "201908"
sep1819 <- calculateMonths(all_wkl, "2018-10-01", "2019-07-31", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
sep1819$ym <- "201909"
oct1819 <- calculateMonths(all_wkl, "2018-11-01", "2019-08-31", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
oct1819$ym <- "201910"
nov1819 <- calculateMonths(all_wkl, "2018-12-01", "2019-09-30", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
nov1819$ym <- "201911"
dec1819 <- calculateMonths(all_wkl, "2019-01-01", "2019-10-31", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
dec1819$ym <- "201912"

all_wkl <- rbind(jul1819, aug1819, sep1819, oct1819, nov1819, dec1819)

write.csv(all_wkl, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FasttrackEngagement.csv", row.names = FALSE)  

core_wkl <- inner_join(tenant_events_temp, core_wkl, by = "ServiceNameEvent")
jul1819 <- calculateMonths(core_wkl, "2018-08-01", "2019-05-31", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
jul1819$ym <- "201907"
aug1819 <- calculateMonths(core_wkl, "2018-09-01", "2019-06-30", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
aug1819$ym <- "201908"
sep1819 <- calculateMonths(core_wkl, "2018-10-01", "2019-07-31", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
sep1819$ym <- "201909"
oct1819 <- calculateMonths(core_wkl, "2018-11-01", "2019-08-31", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
oct1819$ym <- "201910"
nov1819 <- calculateMonths(core_wkl, "2018-12-01", "2019-09-30", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
nov1819$ym <- "201911"
dec1819 <- calculateMonths(core_wkl, "2019-01-01", "2019-10-31", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
dec1819$ym <- "201912"

core_wkl <- rbind(jul1819, aug1819, sep1819, oct1819, nov1819, dec1819)

write.csv(core_wkl, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CoreWorkloads\\FasttrackEngagement.csv", row.names = FALSE)  

modern_comms <- inner_join(tenant_events_temp, modern_comms, by = "ServiceNameEvent")
jul1819 <- calculateMonths(modern_comms, "2018-08-01", "2019-05-31", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
jul1819$ym <- "201907"
aug1819 <- calculateMonths(modern_comms, "2018-09-01", "2019-06-30", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
aug1819$ym <- "201908"
sep1819 <- calculateMonths(modern_comms, "2018-10-01", "2019-07-31", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
sep1819$ym <- "201909"
oct1819 <- calculateMonths(modern_comms, "2018-11-01", "2019-08-31", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
oct1819$ym <- "201910"
nov1819 <- calculateMonths(modern_comms, "2018-12-01", "2019-09-30", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
nov1819$ym <- "201911"
dec1819 <- calculateMonths(modern_comms, "2019-01-01", "2019-10-31", "CSSNoOfMonths", "FRPNoOfMonths", "AutomationNoOfMonths")
dec1819$ym <- "201912"

modern_comms <- rbind(jul1819, aug1819, sep1819, oct1819, nov1819, dec1819)

write.csv(modern_comms, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\FasttrackEngagement.csv", row.names = FALSE)  

