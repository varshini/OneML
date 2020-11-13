library(splitstackshape)
library(dplyr)

#FINAL DATA FRAME

final_data <- data.frame()  #Data for experimentation + expansion data + model anamolies not aligning to boxes

# msxi data for VC boxes 
msxi <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\Adhoc\\MSXi.xlsx", sheet = "Sheet2")
msxi <- msxi %>% select(TPID, OverallPositionID_CM01)
colnames(msxi) <- c("FinalTPID", "Box")

# Experimentation based on the box they come from # 
o365 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\TPID_propensities_v2.csv", sep = ";")
o365 <- o365 %>% select(TPID, recommendation)
colnames(o365) <- c("FinalTPID", "Recommendation")

ca <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\DROutput\\FinalDROutputv2.csv") 
ca <- ca %>% select(FinalTPID, Recommendation)

ems <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\EMSE5SMC_reasons_capped_removed.csv", sep = ";")
ems <- ems %>% select(FinalTPID, RecommendedAction)
colnames(ems) <- c("FinalTPID", "Recommendation")
ems_exp_upsell <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\propensity_EMSE5_SMC.xlsx")
ems <- inner_join(ems_exp_upsell, ems, by = "FinalTPID")
ems_exp <- filter(ems, TypeUpsell == "Extension") 
ems_exp$Scenario <- "EMSExpansion"

e5 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\DROutput_ME5_63020_non1A1B.csv")
e5 <- e5 %>% select(FinalTPID, Recommendation)

# Take SMC list here for final set of TPIDs
smc <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\SMCTPIDs.csv")
colnames(smc)[1] <- c("FinalTPID")
smc <- smc %>% select(FinalTPID)

# Area for experimentation 
area <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\DROutput\\Area.xlsx")
colnames(area)[1] <- "FinalTPID"
area$FinalTPID <- as.integer(area$FinalTPID)

######### 1A + 2A #############
o365 <- inner_join(smc, o365)
o365 <- inner_join(o365, msxi)
o365_1a2a <- filter(o365, Box == 8 | Box == 5) #1990
o365_1a2a$propensity_bucket <- case_when(grepl("High", o365_1a2a$Recommendation) == TRUE ~ "HighPropensity", 
                                         grepl("Medium", o365_1a2a$Recommendation) == TRUE ~ "MediumPropensity")

ca <- inner_join(smc, ca)
ca <- inner_join(ca, msxi)
ca_1a2a <- filter(ca, Box == 8 | Box == 5)
ca_1a2a <- inner_join(ca_1a2a, ca) #2516
ca_1a2a <- inner_join(ca_1a2a, area)
ca_1a2a$propensity_bucket <- case_when(grepl("High", ca_1a2a$Recommendation) == TRUE ~ "HighPropensity", 
                                            grepl("Medium", ca_1a2a$Recommendation) == TRUE ~ "MediumPropensity")

sample <- stratified(ca_1a2a, c("propensity_bucket", "AreaName"), 0.80, bothSets = TRUE, replace = FALSE)
ca_1a2a_treatment_sample <- sample[[1]]
ca_1a2a_treatment_sample$ExpScenario <- "1A2AM365"
ca_1a2a_control_sample <- sample[[2]]
ca_1a2a_control_sample$ExpScenario <- "1A2AM365"

common <- inner_join(o365_1a2a, ca_1a2a, by = "FinalTPID")
only_o365 <- data.frame(setdiff(o365_1a2a$FinalTPID, common$FinalTPID))
colnames(only_o365) <- c("FinalTPID")
only_365_1a2a <- inner_join(only_o365, o365_1a2a, by = "FinalTPID")
only_365_1a2a <- inner_join(only_365_1a2a, area)
sample <- stratified(only_365_1a2a, c("propensity_bucket", "AreaName"), 0.80, bothSets = TRUE, replace = FALSE)
only_365_1a2a_treatment_sample <- sample[[1]]
only_365_1a2a_treatment_sample$ExpScenario <- "1A2AO365"
only_365_1a2a_control_sample <- sample[[2]]
only_365_1a2a_control_sample$ExpScenario <- "1A2AO365"

final_1a2a_treatment <- rbind(only_365_1a2a_treatment_sample, ca_1a2a_treatment_sample) 
final_1a2a_control <- rbind(only_365_1a2a_control_sample, ca_1a2a_control_sample)

######### 1B + 1C #############
ca_1b1c <- filter(ca, Box == 7 | Box == 6) # 3367
ca_1b1c$propensity_bucket <- case_when(grepl("High", ca_1b1c$Recommendation) == TRUE ~ "HighPropensity", 
                                            grepl("Medium", ca_1b1c$Recommendation) == TRUE ~ "MediumPropensity")
ca_1b1c <- inner_join(ca_1b1c, area)
sample <- stratified(ca_1b1c, c("propensity_bucket", "AreaName"), 0.80, bothSets = TRUE, replace = FALSE)
final_1b1c_treatment_sample <- sample[[1]]
final_1b1c_treatment_sample$ExpScenario <- "1B1CM365"
final_1b1c_control_sample <- sample[[2]]
final_1b1c_control_sample$ExpScenario <- "1B1CM365"

# Need to add O365 accounts here without exp
o365_1b1c <- filter(o365, Box == 7 | Box == 6)
only_o365_1b1c <- data.frame(setdiff(o365_1b1c$FinalTPID, ca_1b1c$FinalTPID))
colnames(only_o365_1b1c) <- c("FinalTPID")
only_o365_1b1c <- inner_join(only_o365_1b1c, o365, by = "FinalTPID")
only_o365_1b1c$propensity_bucket <- case_when(grepl("High", only_o365_1b1c$Recommendation) == TRUE ~ "HighPropensity", 
                                       grepl("Medium", only_o365_1b1c$Recommendation) == TRUE ~ "MediumPropensity")
only_o365_1b1c <- inner_join(only_o365_1b1c, area)
only_o365_1b1c$ExpScenario <- "1B1CO365NoExp"

only_o365_1b1c_tpid <- only_o365_1b1c %>% select(FinalTPID)
ca_1b1c_tpid <- ca_1b1c %>% select(FinalTPID)
all_1b1c <- rbind(only_o365_1b1c_tpid, ca_1b1c_tpid)

#EMS Accounts in 1B and 1C
ems_nonexp <- ems %>% filter(TypeUpsell != "Extension")
ems_nonexp <- inner_join(smc, ems_nonexp)
ems_nonexp <- inner_join(ems_nonexp, msxi)
ems_1b1c <- filter(ems_nonexp, Box == 7 | Box == 6)
only_ems_1b1c <- data.frame(setdiff(ems_1b1c$FinalTPID, all_1b1c$FinalTPID))
colnames(only_ems_1b1c) <- c("FinalTPID")
only_ems_1b1c <- inner_join(only_ems_1b1c, ems_nonexp, by = "FinalTPID")
only_ems_1b1c$propensity_bucket <- case_when(grepl("high", only_ems_1b1c$Recommendation) == TRUE ~ "HighPropensity", 
                                              grepl("medium", only_ems_1b1c$Recommendation) == TRUE ~ "MediumPropensity")
only_ems_1b1c <- inner_join(only_ems_1b1c, area)
only_ems_1b1c <- only_ems_1b1c %>% select(FinalTPID, Recommendation, Box, propensity_bucket, AreaName)
only_ems_1b1c$ExpScenario <- "1B1CEMSNoExp"

final_1b1c_treatment_sample <- rbind(final_1b1c_treatment_sample, only_o365_1b1c)
final_1b1c_treatment_sample <- rbind(final_1b1c_treatment_sample, only_ems_1b1c)

######### 2B + 2C + 3B #############

# EMS 
ems_nonexp <- ems %>% filter(TypeUpsell != "Extension")
ems_nonexp <- inner_join(smc, ems_nonexp)
ems_nonexp <- inner_join(ems_nonexp, msxi)
ems_2b2c3b <- filter(ems_nonexp, Box == 4 | Box == 3 | Box == 2) #4661
ems_2b2c3b <- ems_2b2c3b %>% select(FinalTPID, Recommendation, Box)

# E5
e5_23 <- inner_join(smc, e5)
e5_23 <- inner_join(e5_23, msxi)
e5_2b2c3b <- filter(e5_23, Box == 4 | Box == 3 | Box == 2) #2575
e5_2b2c3b$propensity_bucket <- case_when(grepl("high", e5_2b2c3b$Recommendation) == TRUE ~ "HighPropensity", 
                                             grepl("medium", e5_2b2c3b$Recommendation) == TRUE ~ "MediumPropensity")
e5_2b2c3b <- inner_join(e5_2b2c3b, area)
e5_2b2c3b$ExpScenario <- "2B2C3BE5"
sample <- stratified(e5_2b2c3b, c("propensity_bucket", "AreaName"), 0.80, bothSets = TRUE, replace = FALSE)
e5_2b2c3b_treatment_sample <- sample[[1]]
e5_2b2c3b_control_sample <- sample[[2]]

common <- inner_join(ems_2b2c3b, e5_2b2c3b, by = "FinalTPID")
only_ems <- data.frame(setdiff(ems_2b2c3b$FinalTPID, e5_2b2c3b$FinalTPID))
colnames(only_ems) <- c("FinalTPID")
only_ems_2b2c3b <- inner_join(only_ems, ems_2b2c3b) #2844
only_ems_2b2c3b$propensity_bucket <- case_when(grepl("high", only_ems_2b2c3b$Recommendation) == TRUE ~ "HighPropensity", 
                                         grepl("medium", only_ems_2b2c3b$Recommendation) == TRUE ~ "MediumPropensity")
only_ems_2b2c3b <- inner_join(only_ems_2b2c3b, area)
only_ems_2b2c3b$ExpScenario <- "2B2C3BEMS"
sample <- stratified(only_ems_2b2c3b, c("propensity_bucket", "AreaName"), 0.80, bothSets = TRUE, replace = FALSE)
only_ems_2b2c3b_treatment_sample <- sample[[1]]
only_ems_2b2c3b_control_sample <- sample[[2]]

final_2b2c3b_treatment <- rbind(e5_2b2c3b_treatment_sample, only_ems_2b2c3b_treatment_sample) #4334
final_2b2c3b_control <- rbind(e5_2b2c3b_control_sample, only_ems_2b2c3b_control_sample) 

only_ems_2b2c3b_tpid <- only_ems_2b2c3b %>% select(FinalTPID)
e5_2b2c3b_tpid <- e5_2b2c3b %>% select(FinalTPID)
all_2b2c3b <- rbind(only_ems_2b2c3b_tpid, e5_2b2c3b_tpid)

#CA expansion 
ca_2b2c3b <- filter(ca, Box == 4 | Box == 3 | Box == 2) 
only_ca_2b2c3b <- data.frame(setdiff(ca_2b2c3b$FinalTPID, all_2b2c3b$FinalTPID))
colnames(only_ca_2b2c3b) <- c("FinalTPID")
only_ca_2b2c3b <- inner_join(only_ca_2b2c3b, ca, by = "FinalTPID")
only_ca_2b2c3b$propensity_bucket <- case_when(grepl("High", only_ca_2b2c3b$Recommendation) == TRUE ~ "HighPropensity", 
                                               grepl("Medium", only_ca_2b2c3b$Recommendation) == TRUE ~ "MediumPropensity")
only_ca_2b2c3b <- inner_join(only_ca_2b2c3b, area)
only_ca_2b2c3b$ExpScenario <- "2B2C3BM365NoExp"

final_2b2c3b_treatment <- rbind(final_2b2c3b_treatment, only_ca_2b2c3b)

######### 3C and expansion - No experimentation #############

# E5 
e5_exp <- inner_join(smc, e5)
e5_exp <- inner_join(e5_exp, msxi)
e5_exp <- filter(e5_exp, Box == 1) 
e5_exp$propensity_bucket <- case_when(grepl("high", e5_exp$Recommendation) == TRUE ~ "HighPropensity", 
                                              grepl("medium", e5_exp$Recommendation) == TRUE ~ "MediumPropensity")
e5_exp <- inner_join(e5_exp, area)
e5_exp$ExpScenario <- "3CE5NoExp"

#CA 
ca_exp <- inner_join(smc, ca)
ca_exp <- inner_join(ca_exp, msxi)
ca_exp <- filter(ca_exp, Box == 1) 

#Need to do a CA only 
only_ca_3c <- data.frame(setdiff(ca_exp$FinalTPID, e5_exp$FinalTPID))
colnames(only_ca_3c) <- c("FinalTPID")
only_ca_3c <- inner_join(only_ca_3c, ca, by = "FinalTPID")
only_ca_3c$propensity_bucket <- case_when(grepl("High", only_ca_3c$Recommendation) == TRUE ~ "HighPropensity", 
                                              grepl("Medium", only_ca_3c$Recommendation) == TRUE ~ "MediumPropensity")
only_ca_3c <- inner_join(only_ca_3c, area)
only_ca_3c$ExpScenario <- "3CM365NoExp"

final_3c <- rbind(e5_exp, only_ca_3c)

###################################################

final_treatment <- do.call("rbind", list(final_1a2a_treatment, final_1b1c_treatment_sample, final_2b2c3b_treatment, final_3c))
final_control <- do.call("rbind", list(final_1a2a_control, final_1b1c_control_sample, final_2b2c3b_control))

####################################################
# EMS Expansion only across all boxes - No other recommendation

ems_exp <- ems_exp
ems_exp <- inner_join(smc, ems_exp)
ems_exp <- inner_join(ems_exp, msxi)
ems_exp <- ems_exp %>% select(FinalTPID, Recommendation, Box)

ems_exp_only <- data.frame(setdiff(ems_exp$FinalTPID, final_treatment$FinalTPID))
colnames(ems_exp_only) <- c("FinalTPID")
ems_exp_only <- inner_join(ems_exp_only, ems_exp, by = "FinalTPID")
ems_exp_only$propensity_bucket <- case_when(grepl("high", ems_exp_only$Recommendation) == TRUE ~ "HighPropensity", 
                                          grepl("medium", ems_exp_only$Recommendation) == TRUE ~ "MediumPropensity")
ems_exp_only <- inner_join(ems_exp_only, area)
ems_exp_only$ExpScenario <- "AllEMSExpansionNoExp"

final_treatment <- rbind(final_treatment, ems_exp_only)

################################################################
# Final treatment - 11432
# Final Control - 2477

write.csv(final_treatment, "C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\FinalTreatmentTPIDs.csv", row.names = FALSE)
write.csv(final_control, "C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\FinalControlTPIDs.csv", row.names = FALSE)


#Questions to answer - 
# Box count
# <dbl> <int>
# 1     1    22 - Not included
# 2     2    15 - Not included
# 3     3    24 - Not included
# 4     4   186 - Not included
# 5     5    31 - *
# 6     6    19 - * (Added w/o exp)
# 7     7   539 - * (Added w/o exp)
# 8     8  1959 - *

# 2) EMS 
# 1     1   242 - * (Added w/o exp)
# 2     2   286 - * 
# 3     3   509 - *
# 4     4  3971 - *
# 5     5    16 - **
# 6     6   103 - * (Added w/o exp)
# 7     7   653 - * (Added w/o exp)
# 8     8   365 - **

# 3) CA
# Box count
# <dbl> <int>
# 1     1   164 - * (Added w/o exp)
# 2     2   213 - * (Added w/o exp)
# 3     3   258 - * (Added w/o exp)
# 4     4  1567 - * (Added w/o exp)
# 5     5    60 - *
# 6     6   200 - *
# 7     7  3167 - *
# 8     8  2456 - *

# 4) E5
# Box count
# <dbl> <int>
# 1     1   374 - * (Added w/o exp)
# 2     2   405 - 3B - *
# 3     3   432 - 2C - *  
# 4     4  1738 - 2B - *
# 5     5     5 - 2A - Not including 
# 6     6   134 - 1C - Very few non common accounts, excluding

## CHECK ALL CONVERSION % ##

msxi <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\Adhoc\\MSXi.xlsx", sheet = "Sheet2")
colnames(msxi)[1] <- "FinalTPID"
smc <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\ScoringFeaturesFinalv2.csv") %>% select(FinalTPID)
msxi <- inner_join(smc, msxi)
num <- nrow(filter(msxi,((OverallPositionID_CM13 == 4 | OverallPositionID_CM13 == 3 | OverallPositionID_CM13 == 2 ) & OverallPositionID_CM01 == 1)))
denom <- nrow(filter(msxi,((OverallPositionID_CM13 == 4 | OverallPositionID_CM13 == 3 | OverallPositionID_CM13 == 2))))
num/denom                                                                                                                  
     