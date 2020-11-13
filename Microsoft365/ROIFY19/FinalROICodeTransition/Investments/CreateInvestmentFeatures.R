# Putting together all investment features into one feature file for input to databricks across the 3 different models # 

#Fasttrack engmt, Fasttrack events, CSM assignments, CSM engmt, ECIF, Partner, ATS, MCS, Premier
ft_1 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FasttrackEngagement.csv")
ft_2 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FasttrackEventMonths.csv")
ft_3 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FasttrackEventCount.csv")
ft_4 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FTAutomation.csv")
ft_5 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\FTAssignment.csv")
csm_1 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\CSMMonths.csv")
csm_2 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\AllWorkloads\\CSMEngagements.csv")
ecif <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ECIFMonths.csv")
partner <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\AllWorkloads\\PartnerMonths.csv")
ats <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ATSMonths.csv")
mcs <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\AllWorkloads\\MCS.csv")
services <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\AllWorkloads\\Premier.csv")
frp <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\AllWorkloads\\FRPMonths.csv")

#inv_data <- full_join(ft_1, ft_2, by = c("TenantId","ym")) %>% 
#  full_join(., ft_3, by = c("TenantId","ym")) %>% 

inv_data <- full_join(csm_1, csm_2, by =  c("TenantId","ym")) %>% 
  full_join(., ecif, by =  c("TenantId","ym")) %>% 
  full_join(., partner, by =  c("TenantId","ym")) %>% 
  full_join(., ats, by =  c("TenantId","ym")) %>% 
  full_join(., mcs, by =  c("TenantId","ym")) %>% 
  full_join(., services, by =  c("TenantId","ym")) %>% 
  full_join(., frp, by = c("TenantId", "ym"))

mau_pau_final <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\TenantProfileFeatures.csv")
final_data <- left_join(mau_pau_final, inv_data, by = c("TenantId","ym"))  
final_data[is.na(final_data)] <- 0
final_data <- final_data %>% group_by(TenantId, ym) %>% slice(1)
#colnames(final_data)[3] <- "Tpid"

# 
# noedu <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\ROICohortFY20.csv")
# colnames(noedu)[1] <- "Tpid"
# a <- inner_join(noedu, final_data, by = "Tpid") #30.4k tpids
# b <- filter(a, ym == "201912") #29.6k tpids



ft_11 <- select(ft_2, TenantId, ym, contains("t5")) #t5 and t510 months
ft_21 <- select(ft_3, TenantId, ym, contains("t5")) #t5 and t510 counts

ft_data <- full_join(ft_1, ft_2, by = c("TenantId","ym")) %>% 
  full_join(., ft_3, by = c("TenantId","ym")) %>% 
  full_join(., ft_4, by = c("TenantId", "ym")) %>% 
  full_join(., ft_5, by = c("TenantId", "ym"))

mau_pau_tenants <- mau_pau_final %>% select(TenantId, ym)
ft_data <- left_join(mau_pau_tenants, ft_data, by = c("TenantId","ym"))
ft_data[is.na(ft_data)] <- 0

ft_data1 <- left_join(mau_pau_tenants, ft_11, by = c("TenantId","ym"))
ft_data1[is.na(ft_data1)] <- 0

ft_data2 <- left_join(mau_pau_tenants, ft_21, by = c("TenantId","ym"))
ft_data2[is.na(ft_data2)] <- 0

#Final feature file
write.csv(ft_data1, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\FTt5t510FeaturesMonths_AllWorkloads.csv", row.names = FALSE)
write.csv(ft_data2, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\FTt5t510FeaturesCounts_AllWorkloads.csv", row.names = FALSE)
write.csv(ft_data, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\FTAllFeatures_AllWorkloads.csv", row.names = FALSE)
write.csv(final_data, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\Features_AllWorkloadsWithFRP.csv", row.names = FALSE)


####### Core workloads ########

#Fasttrack engmt, Fasttrack events, CSM assignments, CSM engmt, ECIF, Partner, ATS, MCS, Premier
ft_1 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CoreWorkloads\\FasttrackEngagement.csv")
ft_2 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CoreWorkloads\\FasttrackEventMonths.csv")
ft_3 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CoreWorkloads\\FasttrackEventCount.csv")
ft_4 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CoreWorkloads\\FTAutomation.csv")
ft_5 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\FTAssignment.csv")
csm_1 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\CSMMonths.csv")
csm_2 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\CoreWorkloads\\CSMEngagements.csv")
ecif <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ECIFMonths.csv")
partner <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\CoreWorkloads\\PartnerMonths.csv")
ats <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ATSMonths.csv")
mcs <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\CoreWorkloads\\MCS.csv")
frp <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\CoreWorkloads\\FRPMonths.csv")

inv_data <- full_join(csm_1, csm_2, by =  c("TenantId","ym")) %>% 
  full_join(., ecif, by =  c("TenantId","ym")) %>% 
  full_join(., partner, by =  c("TenantId","ym")) %>% 
  full_join(., ats, by =  c("TenantId","ym")) %>% 
  full_join(., mcs, by =  c("TenantId","ym")) %>% 
  full_join(., frp, by = c("TenantId", "ym"))

mau_pau_final <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\TenantProfileFeatures.csv")
final_data <- left_join(mau_pau_final, inv_data, by = c("TenantId","ym"))  
final_data[is.na(final_data)] <- 0
final_data <- final_data %>% group_by(TenantId, ym) %>% slice(1)
#colnames(final_data)[3] <- "Tpid"

# 
# noedu <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\ROICohortFY20.csv")
# colnames(noedu)[1] <- "Tpid"
# a <- inner_join(noedu, final_data, by = "Tpid") #30.4k tpids
# b <- filter(a, ym == "201912") #29.6k tpids

ft_2 <- select(ft_2, TenantId, ym, contains("t10"))
ft_3 <- select(ft_3, TenantId, ym, contains("t10")) 

ft_data <- full_join(ft_1, ft_2, by = c("TenantId","ym")) %>% 
  full_join(., ft_3, by = c("TenantId","ym")) %>% 
  full_join(., ft_4, by = c("TenantId", "ym")) %>% 
  full_join(., ft_5, by = c("TenantId", "ym"))

mau_pau_tenants <- mau_pau_final %>% select(TenantId, ym)
ft_data <- left_join(mau_pau_tenants, ft_data, by = c("TenantId","ym"))
ft_data[is.na(ft_data)] <- 0

#Final feature file
write.csv(ft_data, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\CoreWorkloads\\FTAllFeatures_CoreWorkloads.csv", row.names = FALSE)
write.csv(final_data, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\CoreWorkloads\\Features_CoreWorkloadsWithFRP.csv", row.names = FALSE)

####### Modern Comms workloads ########

#Fasttrack engmt, Fasttrack events, CSM assignments, CSM engmt, ECIF, Partner, ATS, MCS, Premier
ft_1 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\FasttrackEngagement.csv")
ft_2 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\FasttrackEventMonths.csv")
ft_3 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\FasttrackEventCount.csv")
ft_4 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\FTAutomation.csv")
ft_5 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\FTAssignment.csv")
csm_1 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\CSMMonths.csv")
csm_2 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ModernComms\\CSMEngagements.csv")
ecif <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ECIFMonths.csv")
partner <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ModernComms\\PartnerMonths.csv")
ats <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ATSMonths.csv")
mcs <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ModernComms\\MCS.csv")
premier <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ModernComms\\Premier.csv")
frp <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ModernComms\\FRPMonths.csv")

inv_data <- full_join(csm_1, csm_2, by =  c("TenantId","ym")) %>% 
  full_join(., ecif, by =  c("TenantId","ym")) %>% 
  full_join(., partner, by =  c("TenantId","ym")) %>% 
  full_join(., ats, by =  c("TenantId","ym")) %>% 
  full_join(., mcs, by =  c("TenantId","ym")) %>% 
  full_join(., premier, by = c("TenantId", "ym"))  %>% 
  full_join(., frp, by = c("TenantId", "ym"))

mau_pau_final <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\TenantProfileFeatures.csv")
final_data <- left_join(mau_pau_final, inv_data, by = c("TenantId","ym"))  
final_data[is.na(final_data)] <- 0
final_data <- final_data %>% group_by(TenantId, ym) %>% slice(1)
#colnames(final_data)[3] <- "Tpid"

# 
# noedu <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\ROICohortFY20.csv")
# colnames(noedu)[1] <- "Tpid"
# a <- inner_join(noedu, final_data, by = "Tpid") #30.4k tpids
# b <- filter(a, ym == "201912") #29.6k tpids

ft_2 <- select(ft_2, TenantId, ym, contains("t10"))
ft_3 <- select(ft_3, TenantId, ym, contains("t10")) 

ft_data <- full_join(ft_1, ft_2, by = c("TenantId","ym")) %>% 
  full_join(., ft_3, by = c("TenantId","ym")) %>% 
  full_join(., ft_4, by = c("TenantId", "ym")) %>% 
  full_join(., ft_5, by = c("TenantId", "ym"))

mau_pau_tenants <- mau_pau_final %>% select(TenantId, ym)
ft_data <- left_join(mau_pau_tenants, ft_data, by = c("TenantId","ym"))
ft_data[is.na(ft_data)] <- 0

#Final feature file
write.csv(ft_data, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\ModernComms\\FTAllFeatures_ModernComms.csv", row.names = FALSE)
write.csv(final_data, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\ModernComms\\Features_ModernCommsWithFRP.csv", row.names = FALSE)

# Assignment months based on event 
ft_21 <- gather(ft_2, Event, Months, AADPTaskServiceTaskAADPt10:BOT.MicrosoftDefenderATPBOTMicrosoftDefenderATPt10, factor_key=TRUE)
ft_21 <- ft_21 %>% group_by(TenantId, ym) %>% summarise(EventMonths = max(Months))
ft_21 <- left_join(mau_pau_tenants, ft_21, by = c("TenantId","ym"))
ft_21[is.na(ft_21)] <- 0
write.csv(ft_21, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\FTEventBasedAssignmentFeatures_AllWorkloads.csv", row.names = FALSE)

# Only month counts
ft_data <- left_join(mau_pau_tenants, ft_2, by = c("TenantId","ym"))
ft_data[is.na(ft_data)] <- 0
write.csv(ft_data, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\FTt10MonthFeatures_AllWorkloads.csv", row.names = FALSE)



### Analysis on ATS, FT, CSM ###
final_data <- final_data %>% select(TenantId, Tpid, ym, CSSAssignedMonths, ATS_Months, SumMauPrev, SumMau) # contain CSM, ATS
final_data_19 <- filter(final_data, ym == "201912")
ft_event_assnt <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FasttrackEventAssignmentMonths.csv")
ft_event_assnt <- filter(ft_event_assnt, ym == "201912")

data <- full_join(final_data_19, ft_event_assnt, by = "TenantId")
data[is.na(data)] <- 0
# ATS covered 
nrow(filter(data, ATS_Months > 0))#57k
nrow(filter(data, CSSAssignedMonths > 0)) #41k
nrow(filter(data, CSMEngagementMonths > 0)) #36.5k
nrow(filter(data, FTEventMonths > 0)) #39k

data1 <- filter(data, !(ATS_Months == 0 & CSSAssignedMonths == 0 & FTEventMonths == 0))
data1$MauGrowth <- (data1$SumMau - data1$SumMauPrev)/data1$SumMauPrev


# CASE1
case1 <- filter(data1, FTEventMonths != 0 & CSSAssignedMonths != 0 & ATS_Months != 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32% 

# CASE1
case1 <- filter(data1, FTEventMonths != 0 & CSSAssignedMonths != 0 & ATS_Months == 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32%

# CASE1
case1 <- filter(data1, FTEventMonths != 0 & CSSAssignedMonths == 0 & ATS_Months != 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32% 

# CASE1
case1 <- filter(data1, FTEventMonths != 0 & CSSAssignedMonths == 0 & CSMEngagementMonths == 0 & ATS_Months == 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32% 

# CASE1
case1 <- filter(data1, FTEventMonths == 0 & CSSAssignedMonths != 0 & ATS_Months != 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32% 

# CASE1
case1 <- filter(data1, FTEventMonths == 0 & CSSAssignedMonths != 0 & ATS_Months == 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32% 

# CASE1
case1 <- filter(data1, FTEventMonths == 0 & CSSAssignedMonths == 0 & CSMEngagementMonths == 0 & ATS_Months != 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32% 


