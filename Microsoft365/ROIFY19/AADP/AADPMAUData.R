options(stringsAsFactors = FALSE)

aadp_mau <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\AADP\\2019-12_AADPMauTimeseries.ss.csv")

aadp_mau <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\AADP\\2019-12_AADPMau1.csv")

cohort <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\ROICohortFY20.csv")
colnames(cohort)[1] <- "Tpid"
tenants <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FY20MALTenants.csv")

a <- inner_join(cohort,tenants, by = "Tpid")

dec19 <- filter(aadp_mau, Date == "12/31/2019 12:00:00 AM")
b <- inner_join(x, a, by = "TenantId")
b[is.na(b)] <- 0



# ======================== Script  Information =================================
# PURPOSE: Forecasting model for All Up MAU - tenant level using Random forest
#          
#
#
# NOTES:
## =====  environment setup  =================================================
palette(c("black", "#FDB100", "#0078FD",  "#800080", "#CC5500", "#008000", "#400080", "yellowgreen", "#663000", "#FF51FF", "#386CB0", "#BDF9FF", "#FFCA99"))
wxp.col <- c("Word"="#2b579a", "Excel"="#217346", "PowerPoint"="#d24726", "OneNote"="#5c2d91", "Outlook"="#1666b1", "Sway"="#008272")

options(stringsAsFactors=FALSE, cores=19)

require(dplyr)
require(ggplot2)
require(reshape2)
require(scales)
require(lubridate)
require(TTR)
require(quantmod)
require(caTools)
require(MLmetrics)
require(randomForest)
require(caret)
require(CosmosToR)
require(ranger)
require(tidyr)
require(data.table)

Trending <- function(x){
  x[ x == 0 ] <- 1
  n <- length(x)
  if(n<4 || sum(x>0)<4 || any(is.na(x))) return(rep(0, n))
  cts <- caTools::runmean(x, 3, align = 'right')
  ROC(cts, 1, type = "discrete")
}

CalcPrevDate <- function(curDate, lag=3) {
  require(lubridate)
  pd <- as.Date(paste0(curDate, "01"), format="%Y%m%d")-months(lag)
  return(format(pd, "%Y%m"))
}
ymDateDiff <- function(date1, date2) {
  require(lubridate)
  pd <- difftime(as.Date(paste0(date1, "01"), format="%Y%m%d"), as.Date(paste0(date2, "01"), format="%Y%m%d"), units="days")
  return(as.numeric(pd))
}

# =====  Reading all data sources and processing  =================================================

# =====  Feature Eng 1 - MAU, Paid Available Units - Trends, Ratios, Tenure  =====
mau <-  fread("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\AADP\\2019-12_AADPMauTimeseries.ss.csv", header = T, sep = ',')
colnames(mau)[2] <- "TenantId"
mau$ym <- format(as.Date(mau$Date, "%m/%d/%Y"), "%Y%m")
mau[is.na(mau)] <- 0
mau$Workload <- "AADP"
colnames(mau)[3] <- "Mau"

cd <- mau %>% group_by(TenantId, Workload) %>% arrange(ym) %>% mutate(MauTrend=Trending(Mau))
cd <- filter(cd, ym != "201806")
cd <- cd %>% ungroup() %>% select(TenantId, Mau, ym, MauTrend)
colnames(cd) <- c("TenantId", "AADP_Mau", "ym", "AADP_Trend")

cd <- cd %>% mutate_if(is.numeric, funs(ifelse(is.na(.) | !is.finite(.), 0, .))) %>% mutate_if(is.logical, funs(ifelse(is.na(.) | !is.finite(.), F, .)))
mau_final <- cd

# Get the paid available units data
pau1 <-  read.csv('C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\AADP\\TenantProfile\\2018-07_TenantProfile.csv', header = T, sep = ',')
pau2 <-  fread('C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\AADP\\TenantProfile\\2018-08_TenantProfile.csv', header = T, sep = ',')
pau3 <-  read.csv('C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\AADP\\TenantProfile\\2018-09_TenantProfile.csv', header = T, sep = ',')
pau4 <-  read.csv('C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\AADP\\TenantProfile\\2018-10_TenantProfile.csv', header = T, sep = ',')
pau5 <-  read.csv('C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\AADP\\TenantProfile\\2018-11_TenantProfile.csv', header = T, sep = ',')
pau6 <-  read.csv('C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\AADP\\TenantProfile\\2018-12_TenantProfile.csv', header = T, sep = ',')

pau <- bind_rows(pau1, pau2, pau3, pau4, pau5, pau6) 
colnames(pau)[2] <- "TenantId"
pau$ym <- format(as.Date(pau$Date, "%m/%d/%Y"), "%Y%m")
pau_final <- pau

# =====  Feature Eng 3 - FT intent data   =====
intent <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\AADP\\Investments\\FT_Intent.csv") #REPLACE for other models

# =====  TODO - Feature Eng 4 - Investment data- Refer to InvestmentProcessing.R ====
# Read investment file from Investment Processing.R

# =====  Put all the data together for MAL =====

# calculate usage here
mau_pau <- inner_join(mau_final, pau_final, by = c("TenantId", "ym"))

# create prev dataset 
prev <- mau_pau %>% group_by(TenantId) %>% 
  transmute(ym=CalcPrevDate(ym, -12),
            AADP_MauPrev=  AADP_Mau,              
            AADP_TrendPrev =  AADP_Trend,            
            PaidAvailableUnitsPrev = PaidAvailableUnits,   
            AADPEnabledUsersPrev = AADPEnabledUsers,
            AIPEnabledUsersPrev =   AIPEnabledUsers,    
            AATPEnabledUsersPrev =   AATPEnabledUsers,   
            IntuneEnabledUsersPrev    = IntuneEnabledUsers,  
            MCASEnabledUsersPrev = MCASEnabledUsers,
            AADPP2EnabledUsersPrev =   AADPP2EnabledUsers,  
            AIPP2EnabledUsersPrev = AIPP2EnabledUsers,
            AADPPaidAvailableUnitsPrev = AADPPaidAvailableUnits,  
            AIPPaidAvailableUnitsPrev = AIPPaidAvailableUnits,
            AATPPaidAvailableUnitsPrev =  AATPPaidAvailableUnits,
            IntunePaidAvailableUnitsPrev = IntunePaidAvailableUnits,
            MCASPaidAvailableUnitsPrev  = MCASPaidAvailableUnits,
            AADPP2PaidAvailableUnitsPrev = AADPP2PaidAvailableUnits,
            AIPP2PaidAvailableUnitsPrev = AIPP2PaidAvailableUnits)


# create the target variables and select what you need, do a quality check here to ensure right data
mau_pau_final <- mau_final %>% inner_join(prev)

mau_pau_final <- mau_pau_final %>% mutate_if(is.numeric, funs(ifelse(is.na(.) | !is.finite(.), 0, .))) %>% mutate_if(is.logical, funs(ifelse(is.na(.) | !is.finite(.), F, .)))

# create tipping points for workloads
# mau_pau_final <- mau_pau_final %>% mutate(ExoTipP = ifelse(EXOUsagePrev >= 0.4, T, F),
#                                           CoreAppsTipP = ifelse(ODBUsagePrev >= 0.5, T, F),
#                                           OdspTipP = ifelse(ProPlusUsagePrev >= 0.4, T, F), 
#                                           TeamsTipP1 = ifelse(SPOUsagePrev >= 0.1, T, F),
#                                           TeamsTipP2 = ifelse(TeamsUsagePrev >= 0.4, T, F))


# combine with other sources and create final dataset ready for training - tenant_profile, ft_intent
intent$ym <- as.character(intent$ym)
mau_pau_final <- left_join(mau_pau_final, intent, by = c("TenantId", "ym"))
mau_pau_final$BlockedSeats[is.na(mau_pau_final$BlockedSeats)] <- 0
mau_pau_final$NoIntentSeats[is.na(mau_pau_final$NoIntentSeats)] <- 0
mau_pau_final$BlockedPerc[is.na(mau_pau_final$BlockedPerc)] <- 0
mau_pau_final$NoIntentPerc[is.na(mau_pau_final$NoIntentPerc)] <- 0

# Join with other workloads MAU, tenant profile attributes, tenure 
other_tenant_features <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\TenantProfileFeatures.csv")
other_tenant_features <- select(other_tenant_features, -c(SumMauPrev, CoreWklMauPrev, ModernCommsMauPrev, SumPAPrev, CoreWklPAPrev, ModernCommsPAPrev,
                                                          BlockedSeats, NoIntentSeats, BlockedPerc, NoIntentPerc))

mau_pau_final$ym <- as.character(mau_pau_final$ym)
other_tenant_features$ym <- as.character(other_tenant_features$ym)
mau_pau_final <- inner_join(mau_pau_final, other_tenant_features, by = c("TenantId", "ym"))

# check with mal list here finally to make the dataset
length(unique(mau_pau_final$TenantId)) #28711 Tenants
length(unique(mau_pau_final$Tpid.x)) #18150 Tpids

mal <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FY20MALTenants.csv")
colnames(mal)[1] <- "TenantId"
mal <- inner_join(mal, mau_pau_final, by = "TenantId")
length(unique(mal$TenantId)) #125207


# 6) Write out the csv
write.csv(mau_pau_final, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\AADP\\TenantProfileFeatures.csv", row.names = FALSE)




