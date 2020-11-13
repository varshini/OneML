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

tenant_tpid_mapping <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FY20MALTenants.csv")
colnames(tenant_tpid_mapping)[1] <- "TenantId"

# =====  ATS   =====
ats <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ATS.csv")
colnames(ats)[1] <- "Tpid"
ats <- inner_join(tenant_tpid_mapping, ats, by = "Tpid")
ats_months <- ats %>% dplyr::select(TenantId, ATSNoOfMonthsJul, ATSNoOfMonthsAug, ATSNoOfMonthsSep, ATSNoOfMonthsOct, ATSNoOfMonthsNov, ATSNoOfMonthsDec)
ats_months <- gather(ats_months, ym, ATS_Months, ATSNoOfMonthsJul:ATSNoOfMonthsDec)
ats_months$ym <- case_when (ats_months$ym == "ATSNoOfMonthsJul" ~ "201907",
                            ats_months$ym == "ATSNoOfMonthsAug" ~ "201908",
                            ats_months$ym == "ATSNoOfMonthsSep" ~ "201909",
                            ats_months$ym == "ATSNoOfMonthsOct" ~ "201910",
                            ats_months$ym == "ATSNoOfMonthsNov" ~ "201911",
                            ats_months$ym == "ATSNoOfMonthsDec" ~ "201912")

ats_allocation <- ats %>% dplyr::select(TenantId, ATSAvgAllocatedTimeJuly, ATSAvgAllocatedTimeAug, ATSAvgAllocatedTimeSep, ATSAvgAllocatedTimeOct, ATSAvgAllocatedTimeNov, ATSAvgAllocatedTimeDec)
ats_allocation <- gather(ats_allocation, ym, ATS_AvgAllocation, ATSAvgAllocatedTimeJuly:ATSAvgAllocatedTimeDec)
ats_allocation$ym <- case_when (ats_allocation$ym == "ATSAvgAllocatedTimeJuly" ~ "201907",
                                ats_allocation$ym == "ATSAvgAllocatedTimeAug" ~ "201908",
                                ats_allocation$ym == "ATSAvgAllocatedTimeSep" ~ "201909",
                                ats_allocation$ym == "ATSAvgAllocatedTimeOct" ~ "201910",
                                ats_allocation$ym == "ATSAvgAllocatedTimeNov" ~ "201911",
                                ats_allocation$ym == "ATSAvgAllocatedTimeDec" ~ "201912")

write.csv(ats_months, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ATSMonths.csv", row.names = FALSE)
write.csv(ats_allocation, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ATSAllocation.csv", row.names = FALSE)

# =====  ECIF   =====
ecif <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ECIF.csv")
colnames(ecif)[1] <- "Tpid"
ecif <- inner_join(tenant_tpid_mapping, ecif, by = "Tpid")
ecif_months <- ecif %>% dplyr::select(TenantId, ECIFJulMonths, ECIFAugMonths, ECIFSepMonths, ECIFOctMonths, ECIFNovMonths, ECIFDecMonths)
ecif_months <- gather(ecif_months, ym, ECIF_Months, ECIFJulMonths:ECIFDecMonths)
ecif_months$ym <- case_when (ecif_months$ym == "ECIFJulMonths" ~ "201907",
                             ecif_months$ym == "ECIFAugMonths" ~ "201908",
                             ecif_months$ym == "ECIFSepMonths" ~ "201909",
                             ecif_months$ym == "ECIFOctMonths" ~ "201910",
                             ecif_months$ym == "ECIFNovMonths" ~ "201911",
                             ecif_months$ym == "ECIFDecMonths" ~ "201912")
write.csv(ecif_months, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ECIFMonths.csv", row.names = FALSE)

# =====  PARTNER - change source for different combinations for workloads =====
partner <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\Partner_ModernComms.csv")
partner <- gather(partner, ym, PARTNER_Months, PartnerNoOfPaymentMonthsJul:PartnerNoOfPaymentMonthsDec)
partner$ym <- case_when (partner$ym == "PartnerNoOfPaymentMonthsJul" ~ "201907",
                         partner$ym == "PartnerNoOfPaymentMonthsAug" ~ "201908",
                         partner$ym == "PartnerNoOfPaymentMonthsSep" ~ "201909",
                         partner$ym == "PartnerNoOfPaymentMonthsOct" ~ "201910",
                         partner$ym == "PartnerNoOfPaymentMonthsNov" ~ "201911",
                         partner$ym == "PartnerNoOfPaymentMonthsDec" ~ "201912")
write.csv(partner, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\PartnerMonths.csv", row.names = FALSE)

# =====  CSM   =====
csm_asst <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CSMAssignment.csv")
colnames(csm_asst)[1] <- "Tpid"
csm_asst$ym <- case_when (csm_asst$ym == "JulMonths" ~ "201907",
                          csm_asst$ym == "AugMonths" ~ "201908",
                          csm_asst$ym == "SepMonths" ~ "201909",
                          csm_asst$ym == "OctMonths" ~ "201910",
                          csm_asst$ym == "NovMonths" ~ "201911",
                          csm_asst$ym == "DecMonths" ~ "201912")
csm_asst <- inner_join(tenant_tpid_mapping, csm_asst, by = "Tpid")
csm_asst <- csm_asst %>% select(TenantId, ym, CSSAssignedMonths)
write.csv(csm_asst, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\CSMMonths.csv", row.names = FALSE)

# csm engagements - change source for different combinations for workloads
csm_egmt <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\CSMEngagements.csv")
colnames(csm_egmt)[1] <- "Tpid"
csm_egmt$ym <- case_when (csm_egmt$ym == "JulMonths" ~ "201907",
                          csm_egmt$ym == "AugMonths" ~ "201908",
                          csm_egmt$ym == "SepMonths" ~ "201909",
                          csm_egmt$ym == "OctMonths" ~ "201910",
                          csm_egmt$ym == "NovMonths" ~ "201911",
                          csm_egmt$ym == "DecMonths" ~ "201912")
csm_egmt <- inner_join(tenant_tpid_mapping, csm_egmt, by = "Tpid")
csm_egmt <- csm_egmt %>% select(TenantId, ym, CSMEngagementMonths)
write.csv(csm_egmt, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ModernComms\\CSMEngagements.csv", row.names = FALSE)

# ===== SERVICES   =====
#MCS - change source for different combinations for workloads
mcs <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\MCS.csv")
colnames(mcs)[1] <- "Tpid"
mcs$ym <- case_when (mcs$ym == "JulMonths" ~ "201907",
                     mcs$ym == "AugMonths" ~ "201908",
                     mcs$ym == "SepMonths" ~ "201909",
                     mcs$ym == "OctMonths" ~ "201910",
                     mcs$ym == "NovMonths" ~ "201911",
                     mcs$ym == "DecMonths" ~ "201912")
mcs <- inner_join(tenant_tpid_mapping, mcs, by = "Tpid")
write.csv(mcs, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\AllWorkloads\\MCS.csv", row.names = FALSE)

#Premier - change source for different combinations for workloads (No core workloads)
premier <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\Premier.csv")
colnames(premier)[1] <- "Tpid"
premier$ym <- case_when (premier$ym == "JulMonths" ~ "201907",
                         premier$ym == "AugMonths" ~ "201908",
                         premier$ym == "SepMonths" ~ "201909",
                         premier$ym == "OctMonths" ~ "201910",
                         premier$ym == "NovMonths" ~ "201911",
                         premier$ym == "DecMonths" ~ "201912")
premier <- inner_join(tenant_tpid_mapping, premier, by = "Tpid")
write.csv(premier, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ModernComms\\Premier.csv", row.names = FALSE)

# =====  FRP - change source for different combinations for workloads =====
frp <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\FRP_ModernComms.csv")
frp <- gather(frp, ym, FRP_Months, FRPNoOfPaymentMonthsJul:FRPNoOfPaymentMonthsDec)
frp$ym <- case_when (frp$ym == "FRPNoOfPaymentMonthsJul" ~ "201907",
                     frp$ym == "FRPNoOfPaymentMonthsAug" ~ "201908",
                     frp$ym == "FRPNoOfPaymentMonthsSep" ~ "201909",
                     frp$ym == "FRPNoOfPaymentMonthsOct" ~ "201910",
                     frp$ym == "FRPNoOfPaymentMonthsNov" ~ "201911",
                     frp$ym == "FRPNoOfPaymentMonthsDec" ~ "201912")
write.csv(frp, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ModelFeatures\\ModernComms\\FRPMonths.csv", row.names = FALSE)

# =====  TODO - FASTTRACK   =====

#Engagements - Can use as-is (NEEDS TO BE UPDATED IF NEEDED)
ft_egmt <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FasttrackEngagement.csv")

#Events - Can use as-is
ft_egmt <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FasttrackEventMonths.csv")

#### TODO - Combine all investments and make into 3 different files, make all NA as 0 ####




