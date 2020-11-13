# cohort_august <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\Cohort_August.csv")
# colnames(cohort_august)[1] <- "FinalTPID"

actuals <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\Actuals.csv")
colnames(actuals)[1] <- "FinalTPID"
colnames(actuals)[4] <- "August Upsell"
#actuals1 <- inner_join(actuals, cohort_august)

# actuals <- filter(actuals, `August Upsell` != "On Prem")

mal <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\MAL.csv")
colnames(mal)[1] <- "FinalTPID"
ent <- filter(mal, SegmentGroup == "Enterprise")
smc <- filter(mal, SegmentGroup != "Enterprise")

## taking only scored accounts
ca_smc1 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\CASMC.csv") %>% select(FinalTPID)
ca_ent1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\CAEnt.xlsx") %>% select(FinalTPID)
e5_ent1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\ME5Ent.xlsx", sheet = "ME5_Propensities_620_3x_3x") %>% select(FinalTPID)
e5_smc1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\ME5SMC.xlsx") %>% select(FinalTPID)
ems_smc1 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\EMSSMC.csv") %>% select(FinalTPID)
ems_ent1 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\EMSEnt.csv") %>% select(FinalTPID)

final_scored <- do.call("rbind", list(ca_smc1, ca_ent1, e5_ent1, e5_smc1, ems_smc1, ems_ent1))
final_scored <- data.frame(unique(final_scored$FinalTPID))
colnames(final_scored)[1] <- "FinalTPID"
actuals <- inner_join(final_scored, actuals) #1097

##### Combining SMC and Ent #####
o365_smc <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\TPID_propensities_SMC.csv", sep = ";")
colnames(o365_smc) <- c("RowNo", "FinalTPID", "Recommendation", "Propensity", "WhyRecommended", "Conversation")
o365_smc <- o365_smc %>% select(FinalTPID, Recommendation)
o365_ent <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\Enterprise\\OnPrem.csv", sep = ";")
colnames(o365_ent) <- c("RowNo", "FinalTPID", "Recommendation", "Propensity", "WhyRecommended", "Conversation")
o365_ent <- o365_ent %>% select(FinalTPID, Recommendation)
o365 <- rbind(o365_smc, o365_ent)
o365$Model <- "O365"

ems_smc <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\EMSE5SMC_reasons_capped_removed_v2.csv", sep = ";")
colnames(ems_smc) <- c("FinalTPID", "WhyRecommended", "Recommendation", "Propensity", "Conversation")
ems_smc <- ems_smc %>% select(FinalTPID, Recommendation)
ems_ent <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\Enterprise\\EMSE5Ent.csv", sep = ";")
ems_ent <- ems_ent %>% select(FinalTPID, RecommendedAction)
colnames(ems_ent) <- c("FinalTPID", "Recommendation")
ems <- rbind(ems_smc, ems_ent)
ems$Model <- "EMS"

ca_smc <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\FinalDROutputv2.csv") 
ca_smc <- ca_smc %>% select(FinalTPID, Recommendation)
ca_ent <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\Enterprise\\MCA_63020.csv")
ca_ent <- ca_ent %>% select(TPID, Recommendation)
colnames(ca_ent) <- c("FinalTPID", "Recommendation")
ca <- rbind(ca_smc, ca_ent)
ca$Model <- "CA"

e5_smc <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\DROutput_ME5_63020_non1A1B.csv")
e5_smc <- e5_smc %>% select(FinalTPID, Recommendation)
e5_ent <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\Enterprise\\E5Upsell_63020.csv")
e5_ent <- e5_ent %>% select(TPID, Recommendation)
colnames(e5_ent) <- c("FinalTPID", "Recommendation")
e5 <- rbind(e5_smc, e5_ent)
e5$Model <- "E5"

####################################

######## Analysis ########

o365_actual <- filter(actuals,  `August Upsell` == "1B")  # 278

o365_actual <- inner_join(smc, o365_actual, by = "FinalTPID")

# o365_scored <- rbind(o365_smc1, o365_ent1)
# o365_scored <- data.frame(unique(o365_scored$FinalTPID))
# colnames(o365_scored)[1] <- "FinalTPID"
# o365_actual <- inner_join(o365_actual, o365_scored) #345

a <- inner_join(o365_actual, o365) 
nrow(a)
nrow(a)/nrow(o365_actual)
a <- inner_join(o365_actual, ca)  
nrow(a)
nrow(a)/nrow(o365_actual)
a <- inner_join(o365_actual, e5)  
nrow(a)
nrow(a)/nrow(o365_actual)
a <- inner_join(o365_actual, ems)  
nrow(a)
nrow(a)/nrow(o365_actual)

#### M365 E3 ########
m365_actual <- filter(actuals, `August Upsell` == "2B")
m365_actual <- inner_join(smc, m365_actual)

ca_scored <- rbind(ca_smc1, ca_ent1)
ca_scored <- data.frame(unique(ca_scored$FinalTPID))
colnames(ca_scored)[1] <- "FinalTPID"

m365_actual <- inner_join(m365_actual, ca_scored) #345

# ca <- data.frame(unique(ca$FinalTPID))
# colnames(ca)[1] <- "FinalTPID"

a <- inner_join(m365_actual, o365) 
nrow(a)
nrow(a)/nrow(m365_actual)
a <- inner_join(m365_actual, ca) 
nrow(a)
nrow(a)/nrow(m365_actual)
a <- inner_join(m365_actual, e5) 
nrow(a)
nrow(a)/nrow(m365_actual)
a <- inner_join(m365_actual, ems) 
nrow(a)
nrow(a)/nrow(m365_actual)

###############################

m365_e5_actual <- filter(actuals, `August Upsell` == "3C")
e5 <- data.frame(unique(e5$FinalTPID))
colnames(e5)[1] <- "FinalTPID"

m365_e5_actual <- inner_join(ent, m365_e5_actual)

e5_scored <- rbind(e5_smc1, e5_ent1)
e5_scored <- data.frame(unique(e5_scored$FinalTPID))
colnames(e5_scored)[1] <- "FinalTPID"

m365_e5_actual <- inner_join(m365_e5_actual, e5_scored) 

a <- inner_join(m365_e5_actual, o365) 
nrow(a)
nrow(a)/nrow(m365_e5_actual)
a <- inner_join(m365_e5_actual, ca) 
nrow(a)
nrow(a)/nrow(m365_e5_actual)
a <- inner_join(m365_e5_actual, e5) 
nrow(a)
nrow(a)/nrow(m365_e5_actual)
a <- inner_join(m365_e5_actual, ems) 
nrow(a)
nrow(a)/nrow(m365_e5_actual)

###############################

ems_e5_actual <- filter(actuals, `August Upsell` == "3B")
ems <- data.frame(unique(ems$FinalTPID))
colnames(ems)[1] <- "FinalTPID"

ems_e5_actual <- inner_join(smc, ems_e5_actual)

ems_scored <- rbind(ems_smc1, ems_ent1)
ems_scored <- data.frame(unique(ems_scored$FinalTPID))
colnames(ems_scored)[1] <- "FinalTPID"

ems_e5_actual <- inner_join(ems_e5_actual, ems_scored) 

a <- inner_join(ems_e5_actual, o365) 
nrow(a)
nrow(a)/nrow(ems_e5_actual)
a <- inner_join(ems_e5_actual, ca) 
nrow(a)
nrow(a)/nrow(ems_e5_actual)
a <- inner_join(ems_e5_actual, e5) 
nrow(a)
nrow(a)/nrow(ems_e5_actual)
a <- inner_join(ems_e5_actual, ems) 
nrow(a)
nrow(a)/nrow(ems_e5_actual)

all_tpids <- rbind(o365, ems, e5, ca)
all_tpids_1 <- data.frame(unique(all_tpids$FinalTPID))
colnames(all_tpids_1)[1] <- "FinalTPID"
a <- inner_join(all_tpids_1, actuals)
nrow(a)
nrow(a)/nrow(actuals)

all_tpids_smc <- inner_join(all_tpids_1, smc)
all_tpids_ent <- inner_join(all_tpids_1, ent)
actuals_smc <- inner_join(smc, actuals)
actuals_ent <- inner_join(ent, actuals)

a <- inner_join(actuals_smc, all_tpids_smc) 
nrow(a)
nrow(a)/nrow(actuals_smc) #85%

a <- inner_join(actuals_ent, all_tpids_ent) 
nrow(a)
nrow(a)/nrow(actuals_ent) #90%
