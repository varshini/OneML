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
mau <-  fread('C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\MAUData.csv', header = T, sep = ',')
colnames(mau)[3] <- "TenantId"
mau$ym <- format(as.Date(mau$Date, "%m/%d/%Y"), "%Y%m")

cd <- mau %>% group_by(TenantId, Workload) %>% arrange(ym) %>% mutate(MauTrend=Trending(Mau))

cd2 <- cd %>% dplyr::select(TenantId,ym, Workload,Mau)
cd2 <- spread(cd2, Workload, Mau)
cd2[is.na(cd2)] <- 0
colnames(cd2) <- c("TenantId", "ym", "EXO_Mau", "ODB_Mau", "ProPlus_Mau", "SPO_Mau", "Teams_Mau")
cd2$SumMau <- cd2$EXO_Mau + cd2$ODB_Mau + cd2$SPO_Mau + cd2$ProPlus_Mau + cd2$Teams_Mau
cd2$CoreWklMau <- cd2$EXO_Mau + cd2$ODB_Mau + cd2$SPO_Mau + cd2$ProPlus_Mau
cd2$ModernCommsMau <- cd2$Teams_Mau

cd3 <- cd %>% dplyr::select(TenantId,ym, Workload,MauTrend)
cd3 <- spread(cd3, Workload, MauTrend)
colnames(cd3) <- c("TenantId", "ym","EXO_Trend", "ODB_Trend", "ProPlus_Trend", "SPO_Trend", "Teams_Trend")

cd4 <- inner_join(cd2, cd3, by = c("TenantId", "ym"))
cd4 <- cd4 %>% mutate_if(is.numeric, funs(ifelse(is.na(.) | !is.finite(.), 0, .))) %>% mutate_if(is.logical, funs(ifelse(is.na(.) | !is.finite(.), F, .)))
mau_final <- cd4

# Get the paid available units data, calculate Tenure 
pau <-  fread('C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\PAUData.csv', header = T, sep = ',')
colnames(pau)[3] <- "TenantId"
pau$ym <- format(as.Date(pau$Date, "%m/%d/%Y"), "%Y%m")
tenure <- pau %>% select(TenantId, Tpid, ym, Date, FirstPaidStartDate)
pau <- pau %>% select(TenantId, ym, Tpid, PaidAvailableUnits, EXOPaidAvailableUnits, SPOPaidAvailableUnits, OD4BPaidAvailableUnits, TeamsPaidAvailableUnits, ProPlusPaidAvailableUnits)
pau_final <- pau
pau_final$SumPA <- pau_final$EXOPaidAvailableUnits + pau_final$SPOPaidAvailableUnits + pau_final$OD4BPaidAvailableUnits + pau_final$ProPlusPaidAvailableUnits + pau_final$TeamsPaidAvailableUnits
pau_final$CoreWklPA <- pau_final$EXOPaidAvailableUnits + pau_final$SPOPaidAvailableUnits + pau_final$OD4BPaidAvailableUnits + pau_final$ProPlusPaidAvailableUnits
pau_final$ModernCommsPA <- pau_final$TeamsPaidAvailableUnits

#Tenure
tenure$FirstPaidStartDate <- ifelse(tenure$FirstPaidStartDate == "#NULL#", "1/1/1900 12:00:00 AM", tenure$FirstPaidStartDate)
tenure$FirstPaidStartDate <- as.Date(mdy_hms(tenure$FirstPaidStartDate))
tenure$Date <- as.Date(mdy_hms(tenure$Date))

elapsed_months <- function(end_date, start_date) {
  ed <- as.POSIXlt(end_date)
  sd <- as.POSIXlt(start_date)
  12 * (ed$year - sd$year) + (ed$mon - sd$mon)
}
tenure$Tenure <- elapsed_months(tenure$Date,tenure$FirstPaidStartDate)

tenure_final <- tenure %>% select(TenantId, ym, Tpid, Tenure)

# =====  Feature Eng 2 - Geo, industry  =====
tenant_profile <- read.csv('C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\2019-12_TenantProfile.csv', header = T, sep = ',')
tenant_profile <- tenant_profile %>% select(OmsTenantId, MSSalesSubRegionName, MSSalesIndustryName, MSSalesVerticalName)
# Fixing blanks and NA in industry
tenant_profile$MSSalesIndustryName <- ifelse(tenant_profile$MSSalesIndustryName == "", "Unknown", tenant_profile$MSSalesIndustryName)
tenant_profile$MSSalesVerticalName <- ifelse(tenant_profile$MSSalesVerticalName == "", "N/A", tenant_profile$MSSalesVerticalName)
#Converting to factors
tenant_profile$MSSalesSubRegionName <- as.factor(tenant_profile$MSSalesSubRegionName)
tenant_profile$MSSalesIndustryName <- as.factor(tenant_profile$MSSalesIndustryName)
tenant_profile$MSSalesVerticalName <- as.factor(tenant_profile$MSSalesVerticalName)
tenant_profile_final <- tenant_profile

# =====  Feature Eng 3 - FT intent data   =====
intent <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FT_Intent.csv") #REPLACE for other models

# =====  TODO - Feature Eng 4 - Investment data- Refer to InvestmentProcessing.R ====
# Read investment file from Investment Processing.R

# =====  Put all the data together for MAL =====

# calculate usage here
mau_pau <- inner_join(mau_final, pau_final, by = c("TenantId", "ym"))
# join tenure
mau_pau <- inner_join(mau_pau, tenure_final, by = c("TenantId", "ym"))

# create prev dataset 
prev <- mau_pau %>% group_by(TenantId) %>% 
  transmute(ym=CalcPrevDate(ym, -5),
            EXO_MauPrev = EXO_Mau,
            ODB_MauPrev = ODB_Mau,
            ProPlus_MauPrev = ProPlus_Mau,
            SPO_MauPrev = SPO_Mau,
            Teams_MauPrev = Teams_Mau,
            SumMauPrev = SumMau,
            CoreWklMauPrev = CoreWklMau,
            ModernCommsMauPrev = ModernCommsMau,
            EXOPaidAvailableUnitsPrev = EXOPaidAvailableUnits, 
            SPOPaidAvailableUnitsPrev = SPOPaidAvailableUnits, 
            OD4BPaidAvailableUnitsPrev = OD4BPaidAvailableUnits, 
            TeamsPaidAvailableUnitsPrev = TeamsPaidAvailableUnits, 
            ProPlusPaidAvailableUnitsPrev = ProPlusPaidAvailableUnits, 
            SumPAPrev = SumPA, 
            CoreWklPAPrev = CoreWklPA, 
            ModernCommsPAPrev = ModernCommsPA,
            TenurePrev = Tenure,
            EXO_TrendPrev = EXO_Trend, 
            ODB_TrendPrev = ODB_Trend, 
            ProPlus_TrendPrev = ProPlus_Trend, 
            SPO_TrendPrev = SPO_Trend, 
            Teams_TrendPrev = Teams_Trend)

prev <- prev %>% filter(ym %in% c("201907", "201908", "201909", "201910", "201911", "201912"))

# create the target variables and select what you need, do a quality check here to ensure right data
mau_pau_final <- mau_pau %>% inner_join(prev)

# Make usage variables 
mau_pau_final$EXOUsagePrev <- mau_pau_final$EXO_MauPrev/mau_pau_final$EXOPaidAvailableUnitsPrev
mau_pau_final$ODBUsagePrev <- mau_pau_final$ODB_MauPrev/mau_pau_final$OD4BPaidAvailableUnitsPrev
mau_pau_final$ProPlusUsagePrev <- mau_pau_final$ProPlus_MauPrev/mau_pau_final$ProPlusPaidAvailableUnitsPrev
mau_pau_final$SPOUsagePrev <- mau_pau_final$SPO_MauPrev/mau_pau_final$SPOPaidAvailableUnitsPrev
mau_pau_final$TeamsUsagePrev <- mau_pau_final$Teams_MauPrev/mau_pau_final$TeamsPaidAvailableUnitsPrev

mau_pau_final <- mau_pau_final %>% mutate_if(is.numeric, funs(ifelse(is.na(.) | !is.finite(.), 0, .))) %>% mutate_if(is.logical, funs(ifelse(is.na(.) | !is.finite(.), F, .)))

# create tipping points for workloads
mau_pau_final <- mau_pau_final %>% mutate(ExoTipP = ifelse(EXOUsagePrev >= 0.4, T, F),
                                          CoreAppsTipP = ifelse(ODBUsagePrev >= 0.5, T, F),
                                          OdspTipP = ifelse(ProPlusUsagePrev >= 0.4, T, F), 
                                          TeamsTipP1 = ifelse(SPOUsagePrev >= 0.1, T, F),
                                          TeamsTipP2 = ifelse(TeamsUsagePrev >= 0.4, T, F))


#mau_pau_final$NumTrendUp <- rowSums(zx[,6:11] >0.1)

mau_pau_final <- mau_pau_final %>% select(TenantId, ym, Tpid.x, 
                                    EXO_MauPrev, ODB_MauPrev, ProPlus_MauPrev, SPO_MauPrev, Teams_MauPrev, SumMauPrev, CoreWklMauPrev, ModernCommsMauPrev, 
                                    EXO_TrendPrev, ODB_TrendPrev, ProPlus_TrendPrev, SPO_TrendPrev, Teams_TrendPrev,
                                    EXOPaidAvailableUnitsPrev, SPOPaidAvailableUnitsPrev, OD4BPaidAvailableUnitsPrev, TeamsPaidAvailableUnitsPrev, 
                                    ProPlusPaidAvailableUnitsPrev, SumPAPrev, CoreWklPAPrev, ModernCommsPAPrev,TenurePrev,
                                    EXOUsagePrev, ODBUsagePrev, ProPlusUsagePrev, SPOUsagePrev, TeamsUsagePrev, 
                                    ExoTipP, CoreAppsTipP, OdspTipP, TeamsTipP1, TeamsTipP2, 
                                    SumMau, CoreWklMau, ModernCommsMau)


# combine with other sources and create final dataset ready for training - tenant_profile, ft_intent
colnames(tenant_profile_final)[1] <- "TenantId"
mau_pau_final <- inner_join(mau_pau_final, tenant_profile_final, by = "TenantId") 
intent$ym <- as.character(intent$ym)
mau_pau_final <- left_join(mau_pau_final, intent, by = c("TenantId", "ym"))
mau_pau_final$BlockedSeats[is.na(mau_pau_final$BlockedSeats)] <- 0
mau_pau_final$NoIntentSeats[is.na(mau_pau_final$NoIntentSeats)] <- 0
mau_pau_final$BlockedPerc[is.na(mau_pau_final$BlockedPerc)] <- 0
mau_pau_final$NoIntentPerc[is.na(mau_pau_final$NoIntentPerc)] <- 0

# check with mal list here finally to make the dataset
length(unique(mau_pau_final$TenantId)) #125207 Tenants
length(unique(mau_pau_final$Tpid.x)) #35170 Tpids

mal <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FY20MALTenants.csv")
colnames(mal)[1] <- "TenantId"
mal <- inner_join(mal, mau_pau_final, by = "TenantId")
length(unique(mal$TenantId)) #125207


# 6) Write out the csv

write.csv(mau_pau_final, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Modeling\\TenantProfileFeatures_6months.csv", row.names = FALSE)




