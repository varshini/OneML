# cohort_august <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\Cohort_August.csv")
# colnames(cohort_august)[1] <- "FinalTPID"

actuals <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\Q1EndActuals.csv")
colnames(actuals)[1] <- "FinalTPID"
actuals <- filter(actuals, IS.UPSELL == 1)

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
#Overall Models Coverage #
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

s2500 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\S2500.csv")
colnames(s2500)[1] <- "FinalTPID"

all_tpids_2500 <- inner_join(all_tpids_ent, s2500)
actuals_2500 <- inner_join(s2500, actuals)
a <- inner_join(actuals_2500, all_tpids_2500) 
nrow(a)
nrow(a)/nrow(actuals_2500) #90%

################################
# Per model #

all_data <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\Q1EndActuals.csv")
colnames(all_data)[1] <- "FinalTPID"
all_data <- filter(all_data, IS.UPSELL == 0)

#### M365 E5 ####
e5_possible <- filter(all_data, SEP.BOX %in% c("2B", "2C", "3B")) #12k
e5_propensity <- inner_join(e5_possible, e5, by = "FinalTPID")
e5_propensity %>% group_by(SEGMENT) %>% summarise(count = n())

#### M365 E3 ####
e3_possible <- filter(all_data, SEP.BOX %in% c("1A", "2A", "1B", "1C")) #20.4k
e3_propensity <- inner_join(e3_possible, ca, by = "FinalTPID") #6991
e3_propensity %>% group_by(SEGMENT) %>% summarise(count = n())

#### O365 ####
o365_possible <- filter(all_data, SEP.BOX %in% c("1A", "2A")) #12.2k
o365_propensity <- inner_join(o365_possible, o365, by = "FinalTPID") #2381
only_o365 <- data.frame(setdiff(o365_propensity$FinalTPID, e3_propensity$FinalTPID))
colnames(only_o365)[1] <- "FinalTPID" #1336
only_o365 <- inner_join(only_o365, all_data)
only_o365 %>% group_by(SEGMENT) %>% summarise(count = n())
only_o365$Recommendation <- "Dummy"
only_o365$Model <- "Dummy"

#### EMS ####
ems_possible <- filter(all_data, SEP.BOX %in% c("1B", "1C", "2B", "2C")) #19.1k
ems_propensity <- inner_join(ems_possible, ems, by = "FinalTPID") #8120
only_ems <- data.frame(setdiff(ems_propensity$FinalTPID, e5_propensity$FinalTPID))
colnames(only_ems)[1] <- "FinalTPID" 
only_ems <- data.frame(setdiff(only_ems$FinalTPID, e3_propensity$FinalTPID))
colnames(only_ems)[1] <- "FinalTPID" #4672
only_ems <- inner_join(only_ems, all_data)
only_ems %>% group_by(SEGMENT) %>% summarise(count = n())
only_ems$Recommendation <- "Dummy"
only_ems$Model <- "Dummy"

#### In total ####
all_tpids <- rbind(o365, ems, e5, ca)
all_tpids_1 <- data.frame(unique(all_tpids$FinalTPID))
colnames(all_tpids_1)[1] <- "FinalTPID"
a <- inner_join(all_tpids_1, all_data)
nrow(a)
nrow(a)/nrow(all_data)

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

correct_upsell <- rbind(e5_propensity, e3_propensity, only_o365, only_ems) # 16.8k
correct_upsell %>% group_by(SEGMENT) %>% summarise(count = n())
correct_upsell %>% group_by(Anniversay.Renewal.Flag) %>% summarise(count = n())

nrow(filter(correct_upsell, MW.PIPE > 0))
sum(correct_upsell$MW.PIPE)

no_pipe <- filter(correct_upsell, MW.PIPE == 0)
a <- no_pipe %>% group_by(AREA) %>% summarise(count = n())


correct_upsell$finalSegment <- ifelse(correct_upsell$SEGMENT %in% c("Enterprise Commercial", "Enterprise Public Sector", 
                                                                    "Enterprise Growth"), "Enterprise", "SMC")
                                                                    
a <- correct_upsell %>% group_by(AREA, finalSegment) %>% summarise(count = n())

