library(dplyr)
library(lubridate)
library(CosmosToR)
library(tidyr)

################################
# Picking the set of FT events #
tenant_level <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\TenantEvents.csv")
tpid_level <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\TPIDEvents.csv")
tenant_level1 <- tenant_level

tenant_level1 <- tenant_level1 %>% filter(MonthEndEventDate != "#NULL#")
tenant_level1$MonthEndDate <- mdy_hms(tenant_level1$MonthEndEventDate)

tenant_level_filtered <- tenant_level1 %>% filter(MonthEndDate > "2018-06-30" & MonthEndDate < "2019-07-01")
tenant_level_filtered <- tenant_level_filtered %>% filter(EventCategory %in%
                                                          c('Admin Center Wizards',
                                                          'Admin Center-BOT',
                                                          'Admin Center-Setup Guide',
                                                          'Adoption Workshop',
                                                          'BOT',
                                                          'BOT Driven Wizards',
                                                          'E-Mail Your Org',
                                                          'FastTrack Automation',
                                                          'Field - CSU Actions',
                                                          'FT Engagement',
                                                          'FT Migration Data',
                                                          'FT Playbook Events',
                                                          'FT Playbook Milestones',
                                                          'FTC Tasks',
                                                          'FTOP Wizard',
                                                          'Service Task',
                                                          'SSAT'))

tenant_level_filtered <- tenant_level_filtered %>% filter(ServiceNameEvent %in%
                                                            c('Tenant Level','ODB','SPO','Exchange: Outlook Mobile',
                                                              'Intune','Exchange','Yammer','OPP','SfB',
                                                              'SfB Cloud PSTN Conferencing','Teams','AADP','Office 365','SfB Cloud PBX',
                                                              'SfB PSTN Calling'))

# Removing very rare occuring events - Mean - 2.5k, median - 43
summary <- tenant_level_filtered %>% group_by(EventTitle) %>% summarise(count = n())
nrow(filter(summary, count < 10)) #-292
nrow(filter(summary, count < 20)) #-394
nrow(filter(summary, count < 30)) #-447
nrow(filter(summary, count < 40)) #-482
nrow(filter(summary, count < 50)) #-514
filtered_events <- summary %>% filter(count > 30) %>% dplyr::select(EventTitle)

# filter out very small count events 

# Featurize the events - two types (1 or 0, # of events) - START FROM HERE
tenant_level_filtered$EventLabel <- 1
tenant_level_features <- tenant_level_filtered %>% group_by(TenantId, EventTitle) %>% summarise(#EventLabel = max(EventLabel))
                                                                                                EventCount = n())

# long to wide 
tenant_level_features1 <- tenant_level_features %>% spread(EventTitle, EventCount)
tenant_level_features1[is.na(tenant_level_features1)] <- 0

tenant_level_features2 <- tenant_level_features %>% spread(EventTitle, EventLabel)
tenant_level_features2[is.na(tenant_level_features2)] <- 0

################################

# Pulling MAU for FY18 and FY19 for MAL

vc <- vc_connection('https://cosmos14.osdinfra.net/cosmos/ACE.proc/')
streamPath <- "local/Projects/ROIFY19/FY18_19_MAU.ss"
mau_data <- ss_all(vc, streamPath) # 100k tenants from 32k tpids with tenants and Office MAU as of FY18 and FY19
mau_data$Growth <- (mau_data$MAUFY19 - mau_data$MAUFY18)/mau_data$MAUFY18
mau_data$GrowthLabel <- ifelse(mau_data$Growth >= 0.01, "1", "0") # 56218 with label 1, 43931 with label 0
colnames(mau_data)[1] <- "TenantId"
mau_data <- mau_data %>% dplyr::select(TenantId, GrowthLabel)

################################
# Output variable - 0 or 1 based on >1% MAU Growth?
# Input variables - 0 or 1 based on if the event happened or not 

final_data <- left_join(mau_data, tenant_level_features2, by = "TenantId")
final_data[is.na(final_data)] <- 0

################################
# Logistic Regression Model 
library(aod)
final_data$GrowthLabel <- factor(final_data$GrowthLabel)
vars <- paste("Var",1:10,sep="")

final_data1 <- head(final_data, 1000)
mylogit <- glm(GrowthLabel~., data = final_data, family = "binomial")

