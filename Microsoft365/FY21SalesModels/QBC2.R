actuals <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\H1Upsell.csv")
colnames(actuals)[1] <- "FinalTPID"

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
all_tpids_2 <- data.frame(unique(all_tpids$FinalTPID))
colnames(all_tpids_2)[1] <- "FinalTPID"
a <- inner_join(all_tpids_2, actuals)
nrow(a)
nrow(a)/nrow(actuals)

#######################
# Per model #

all_data <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\H1All.csv")
colnames(all_data)[1] <- "FinalTPID"
all_data$MW.PIPE[is.na(all_data$MW.PIPE)] <- 0
all_data <- filter(all_data, IsUpsell == 0)

#### M365 E5 ####
e5_possible <- filter(all_data, Dec %in% c("2B", "2C", "3B")) #11.7k
e5_propensity <- inner_join(e5_possible, e5, by = "FinalTPID") #3626
e5_propensity %>% group_by(SEGMENT) %>% summarise(count = n())

#### M365 E3 ####
e3_possible <- filter(all_data, Dec %in% c("1A", "2A", "1B", "1C")) #19.7k
e3_propensity <- inner_join(e3_possible, ca, by = "FinalTPID") #6606
e3_propensity %>% group_by(SEGMENT) %>% summarise(count = n())

#### O365 ####
o365_possible <- filter(all_data, Dec %in% c("1A", "2A")) #13.2k
o365_propensity <- inner_join(o365_possible, o365, by = "FinalTPID") #2628
only_o365 <- data.frame(setdiff(o365_propensity$FinalTPID, e3_propensity$FinalTPID))
colnames(only_o365)[1] <- "FinalTPID" #1336
only_o365 <- inner_join(only_o365, all_data)
only_o365 %>% group_by(SEGMENT) %>% summarise(count = n())
only_o365$Recommendation <- "Dummy"
only_o365$Model <- "Dummy"

#### EMS ####
ems_possible <- filter(all_data, Dec %in% c("1B", "1C", "2B", "2C")) #19.1k
ems_propensity <- inner_join(ems_possible, ems, by = "FinalTPID") #7711
only_ems <- data.frame(setdiff(ems_propensity$FinalTPID, e5_propensity$FinalTPID))
colnames(only_ems)[1] <- "FinalTPID" 
only_ems <- data.frame(setdiff(only_ems$FinalTPID, e3_propensity$FinalTPID))
colnames(only_ems)[1] <- "FinalTPID" #4672
only_ems <- inner_join(only_ems, all_data)
only_ems %>% group_by(SEGMENT) %>% summarise(count = n())
only_ems$Recommendation <- "Dummy"
only_ems$Model <- "Dummy"

#### In total ####
correct_upsell <- rbind(e5_propensity, e3_propensity, only_o365, only_ems) #16k
correct_upsell %>% group_by(SEGMENT) %>% summarise(count = n())

mal <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\MAL.csv")
colnames(mal)[1] <- "FinalTPID" 
correct_upsell <- inner_join(correct_upsell, mal, by = "FinalTPID")

b <- correct_upsell %>% group_by(AreaName) %>% summarise(count = n())

correct_upsell$MW.PIPE[is.na(correct_upsell$MW.PIPE)] <- 0
nrow(filter(correct_upsell, MW.PIPE > 0))
no_pipe <- filter(correct_upsell, MW.PIPE == 0)
a <- no_pipe %>% group_by(AREA) %>% summarise(count = n())


# correct_upsell$finalSegment <- ifelse(correct_upsell$SEGMENT %in% c("Enterprise Commercial", "Enterprise Public Sector", 
#                                                                     "Enterprise Growth"), "Enterprise", "SMC")
# a <- correct_upsell %>% group_by(AREA, finalSegment) %>% summarise(count = n())


vuc <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\VUC_Q2.csv")
colnames(vuc)[1] <- "FinalTPID"
y <- inner_join(correct_upsell, vuc, by = "FinalTPID")
