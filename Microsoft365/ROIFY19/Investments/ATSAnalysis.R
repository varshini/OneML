### Analysis on ATS, FT, CSM ###
final_data <- final_data %>% select(TenantId, Tpid, ym, CSSAssignedMonths, ATS_Months, SumMauPrev, SumMau) # contain CSM, ATS
final_data_19 <- filter(final_data, ym == "201912")
ft_event_assnt <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FasttrackEventAssignmentMonths.csv")
ft_event_assnt <- filter(ft_event_assnt, ym == "201912")

data <- full_join(final_data_19, ft_event_assnt, by = "TenantId")
data[is.na(data)] <- 0

data <- data %>% group_by(Tpid) %>% summarise(SumMau = sum(SumMau), 
                                              SumMauPrev = sum(SumMauPrev), 
                                              CSSAssignedMonths = max(CSSAssignedMonths), 
                                              ATS_Months = max(ATS_Months), 
                                              FTEventMonths = max(FTEventMonths))
data$MauGrowth <- ((data$SumMau - data$SumMauPrev)/data$SumMauPrev) * 100


###stats
nrow(filter(data, ATS_Months > 0))#57k
nrow(filter(data, CSSAssignedMonths > 0)) #41k
nrow(filter(data, CSMEngagementMonths > 0)) #36.5k
nrow(filter(data, FTEventMonths > 0)) #39k
###

t1500 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\T1500.csv")
colnames(t1500)[1] <- "Tpid"
t1500$s1500 <- 1
data1 <- left_join(data, t1500, by = "Tpid")
data1$s1500[is.na(data1$s1500)] <- 0

#data1<- data
data1 <- data %>% filter(MauGrowth < 2)
data1$MauGrowth[is.infinite(data1$MauGrowth)] <- 0
data1$MauGrowth[is.nan(data1$MauGrowth)] <- 0




modern_comms <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\ATSAnalysis_Teams.csv")
#all_wkl <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\ATSAnalysis_AllWkl.csv")

data1 <- modern_comms
data1 <- data1 %>% filter(ModernCommsMauPrev > 100)
data1 <- data1 %>% filter(s1500 == 1)

# CASE1
case1 <- filter(data1, FTEventMonths != 0 & CSSAssignedMonths != 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32% 
a <- case1$ModernCommsMau - case1$ModernCommsMauPrev
mean(a)
median(a)

# CASE1
case1 <- filter(data1, FTEventMonths != 0 & CSSAssignedMonths != 0 & ATS_Months == 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32%
a <- case1$ModernCommsMau - case1$ModernCommsMauPrev
mean(a)
median(a)

# CASE1
case1 <- filter(data1, FTEventMonths != 0 & CSSAssignedMonths == 0 )
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32% 
a <- case1$ModernCommsMau - case1$ModernCommsMauPrev
mean(a)
median(a)

# CASE1
case1 <- filter(data1, FTEventMonths != 0 & CSSAssignedMonths == 0 & ATS_Months == 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32% 
a <- case1$ModernCommsMau - case1$ModernCommsMauPrev
mean(a)
median(a)

# CASE1
case1 <- filter(data1, FTEventMonths == 0 & CSSAssignedMonths != 0 & ATS_Months != 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32% 
a <- case1$ModernCommsMau - case1$ModernCommsMauPrev
mean(a)
median(a)

# CASE1
case1 <- filter(data1, FTEventMonths == 0 & CSSAssignedMonths != 0 & ATS_Months == 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32% 
a <- case1$ModernCommsMau - case1$ModernCommsMauPrev
mean(a)
median(a)

# CASE1
case1 <- filter(data1, FTEventMonths == 0 & CSSAssignedMonths == 0 & ATS_Months != 0)
length(unique(case1$Tpid)) # 3091
nrow(case1)
mean(case1$MauGrowth) # 24%
median(case1$MauGrowth) #32% 
a <- case1$ModernCommsMau - case1$ModernCommsMauPrev
mean(a)
median(a)


