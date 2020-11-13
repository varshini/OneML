# Featurize CSM csm_asst - 2 features - 1) Assignment csm_asst - 1/0 (# of months?) 2) Months of Engagement, # of engagements?

library(dplyr)
library(lubridate)
library(tidyr)

# Assignment - 
csm_fy19 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\CSMFY19.csv")
colnames(csm_fy19)[1] <- "TPID"
csm_fy19 <- csm_fy19 %>% select(TPID)
csm_fy19$Aug18 <- 1
csm_fy19$Sep18 <- 1
csm_fy19$Oct18 <- 1
csm_fy19$Nov18 <- 1
csm_fy19$Dec18 <- 1
csm_fy19$Jan19 <- 1
csm_fy19$Feb19 <- 1
csm_fy19$Mar19 <- 1
csm_fy19$Apr19 <- 1
csm_fy19$May19 <- 1
csm_fy19$Jun19 <- 1

csm_fy <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\CSMFY.csv")
colnames(csm_fy)[1] <- "TPID"
csm_fy <- csm_fy %>% select(TPID)
csm_fy$Jul19 <- 1
csm_fy$Aug19 <- 1
csm_fy$Sep19 <- 1
csm_fy$Oct19 <- 1
csm_fy$Nov19 <- 1
csm_fy$Dec19 <- 1

csm_asst <- full_join(csm_fy19, csm_fy, by = "TPID")
csm_asst[is.na(csm_asst)] <- 0

csm_asst$JulMonths <- csm_asst$Aug18 + csm_asst$Sep18 + csm_asst$Oct18 + csm_asst$Nov18 + csm_asst$Dec18 + csm_asst$Jan19 + csm_asst$Feb19 + csm_asst$Mar19 + csm_asst$Apr19 + csm_asst$May19
csm_asst$AugMonths <- csm_asst$Sep18 + csm_asst$Oct18 + csm_asst$Nov18 + csm_asst$Dec18 + csm_asst$Jan19 + csm_asst$Feb19 + csm_asst$Mar19 + csm_asst$Apr19 + csm_asst$May19 + csm_asst$Jun19
csm_asst$SepMonths <- csm_asst$Oct18 + csm_asst$Nov18 + csm_asst$Dec18 + csm_asst$Jan19 + csm_asst$Feb19 + csm_asst$Mar19 + csm_asst$Apr19 + csm_asst$May19 + csm_asst$Jun19 + csm_asst$Jul19
csm_asst$OctMonths <- csm_asst$Nov18 + csm_asst$Dec18 + csm_asst$Jan19 + csm_asst$Feb19 + csm_asst$Mar19 + csm_asst$Apr19 + csm_asst$May19 + csm_asst$Jun19 + csm_asst$Jul19 + csm_asst$Aug19
csm_asst$NovMonths <- csm_asst$Dec18 + csm_asst$Jan19 + csm_asst$Feb19 + csm_asst$Mar19 + csm_asst$Apr19 + csm_asst$May19 + csm_asst$Jun19 + csm_asst$Jul19 + csm_asst$Aug19 + csm_asst$Sep19
csm_asst$DecMonths <- csm_asst$Jan19 + csm_asst$Feb19 + csm_asst$Mar19 + csm_asst$Apr19 + csm_asst$May19 + csm_asst$Jun19 + csm_asst$Jul19 + csm_asst$Aug19 + csm_asst$Sep19 + csm_asst$Oct19
csm_asst <- csm_asst %>% select(TPID, JulMonths, AugMonths, SepMonths, OctMonths, NovMonths, DecMonths)

data1 <- gather(csm_asst, MonthYear, CSSAssignedMonths, JulMonths:DecMonths)
colnames(data1)[2] <- "ym"

write.csv(data1, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CSMAssignment.csv", row.names = FALSE)
  
# Taking engagements with milestones and filtered for specific workloads

csm_eng <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\CSMDataFiltered.csv")
colnames(csm_eng)[1] <- "EngagementCreatedDate"
csm_eng$StartDate <- mdy(csm_eng$EngagementCreatedDate)
csm_eng$EndDate <- mdy(csm_eng$EngagementEstimatedCompletionDate)
csm_eng$FY19 <- ifelse(csm_eng$StartDate <= "2019-06-30" & csm_eng$EndDate > "2018-06-30", 1, 0)
csm_eng$FY20 <- ifelse(csm_eng$EndDate > "2019-06-30", 1, 0)

csm_overall <- csm_eng 
csm_core <- csm_eng %>% filter(Workload %in% c("SharePoint Online", "Exchange Online", "EM: InTune", "O365 ProPlus", "OneDrive for Business", "EM: AAD", "Outlook Mobile",
                                               "Advanced Security Capabilities", "Advanced Voice Capabilities", "Advanced Analytics Capabilities", "Apps: App Innovation (App Service, Serverless)",             
                                               "Infra: Storage & File Systems (inc.ANF)","Infra: Azure Security (Sentinel, Security Center, Firewall)", "Apps:Â App Modernization (Website/Webapp)"))
csm_moderncomms <- csm_eng %>% filter(Workload %in% c("Teams", "Advanced Voice Capabilities")) 


# Repeat for each combination of workload #

data <- csm_moderncomms

data$Aug2018 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2018-08-01"), ymd("2018-08-31"))) & data$FY19 == 1, 1, 0)
data$Sep2018 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2018-09-01"), ymd("2018-09-30"))) & data$FY19 == 1, 1, 0)
data$Oct2018 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2018-10-01"), ymd("2018-10-31"))) & data$FY19 == 1, 1, 0)
data$Nov2018 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2018-11-01"), ymd("2018-11-30"))) & data$FY19 == 1, 1, 0)
data$Dec2018 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2018-12-01"), ymd("2018-12-31"))) & data$FY19 == 1, 1, 0)
data$Jan2019 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2019-01-01"), ymd("2019-01-31"))) & data$FY19 == 1, 1, 0)
data$Feb2019 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2019-02-01"), ymd("2019-02-28"))) & data$FY19 == 1, 1, 0)
data$Mar2019 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2019-03-01"), ymd("2019-03-31"))) & data$FY19 == 1, 1, 0)
data$Apr2019 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2019-04-01"), ymd("2019-04-30"))) & data$FY19 == 1, 1, 0)
data$May2019 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2019-05-01"), ymd("2019-05-31"))) & data$FY19 == 1, 1, 0)
data$Jun2019 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2019-06-01"), ymd("2019-06-30"))) & data$FY19 == 1, 1, 0)
data$Jul2019 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2019-07-01"), ymd("2019-07-31"))) & data$FY20 == 1, 1, 0)
data$Aug2019 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2019-08-01"), ymd("2019-08-31"))) & data$FY20 == 1, 1, 0)
data$Sep2019 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2019-09-01"), ymd("2019-09-30"))) & data$FY20 == 1, 1, 0)
data$Oct2019 <- ifelse(int_overlaps(interval(data$StartDate, data$EndDate), interval(ymd("2019-10-01"), ymd("2019-10-31"))) & data$FY20 == 1, 1, 0)

data <- data %>% group_by(TPID) %>% summarise(Aug2018 = max(Aug2018), Sep2018 = max(Sep2018), Oct2018 = max(Oct2018),Nov2018 = max(Nov2018),Dec2018 = max(Dec2018),
                                                          Jan2019 = max(Jan2019),Feb2019 = max(Feb2019),Mar2019 = max(Mar2019),Apr2019 = max(Apr2019),May2019 = max(May2019),
                                                          Jun2019 = max(Jun2019),Jul2019 = max(Jul2019),Aug2019 = max(Aug2019),Sep2019 = max(Sep2019),Oct2019 = max(Oct2019))

data$JulMonths <- data$Aug2018 + data$Sep2018 + data$Oct2018 + data$Nov2018 + data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019
data$AugMonths <- data$Sep2018 + data$Oct2018 + data$Nov2018 + data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019
data$SepMonths <- data$Oct2018 + data$Nov2018 + data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019 + data$Jul2019
data$OctMonths <- data$Nov2018 + data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019 + data$Jul2019 + data$Aug2019
data$NovMonths <- data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019 + data$Jul2019 + data$Aug2019 + data$Sep2019
data$DecMonths <- data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019 + data$Jul2019 + data$Aug2019 + data$Sep2019 + data$Oct2019
data <- data %>% select(TPID, JulMonths, AugMonths, SepMonths, OctMonths, NovMonths, DecMonths)

data <- gather(data, MonthYear, CSMEngagementMonths, JulMonths:DecMonths)
colnames(data)[2] <- "ym"

write.csv(data, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\CSMEngagements.csv", row.names = FALSE)
