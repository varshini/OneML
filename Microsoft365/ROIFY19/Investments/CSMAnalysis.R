library(dpylr)
library(lubridate)

csm_eng <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\CSMDataRawv2.csv") 
csm_eng <- filter(csm_eng,!is.na(TPAccountID))
csm_eng$StartDate <- mdy(csm_eng$EngagementCreatedDate)
csm_eng$EndDate <- mdy(csm_eng$EngagementEstimatedCompletionDate)
colnames(csm_eng)[6] <- "TPID" 

#FY20 - Pinned and swarmed
csm_20 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\CSMFY20.csv") 
colnames(csm_20)[1] <- "TPID"
csm_eng_20 <- filter(csm_eng, EndDate > "2019-06-30")
a <- inner_join(csm_eng_20, csm_20, by = "TPID")

#FY19
csm_fy19 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\CSMFY19.csv") 
colnames(csm_fy19)[1] <- "TPID"
csm_eng_19 <- filter(csm_eng, StartDate <= "2019-06-30" & EndDate > "2018-06-30")
b <- inner_join(csm_eng_19, csm_fy19, by = "TPID")


# status and date not mataching
a <- filter(csm_eng, EndDate <= "2019-06-30" & EngagementStatus == "In Progress")


b <- inner_join(a, csm_eng, by = c("TPID", "Workload"))
