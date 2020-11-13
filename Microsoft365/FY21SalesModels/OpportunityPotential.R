mal <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\MAL.csv")
colnames(mal)[1] <- "FinalTPID"
smc <- filter(mal, SegmentGroup != "Enterprise")

o365_smc <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\TPID_propensities_SMC.csv", sep = ";")
colnames(o365_smc) <- c("RowNo", "FinalTPID", "Recommendation", "Propensity", "WhyRecommended", "Conversation")
o365_smc <- o365_smc %>% select(FinalTPID, Recommendation, Propensity)
o365_smc <- inner_join(o365_smc, smc)
o365_smc$Revenue <- 300 * 20 * 12 * (o365_smc$Propensity/100)
nrow(o365_smc) #2793
sum(o365_smc$Revenue)  # 86M


ems_smc <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\EMSE5SMC_reasons_capped_removed_v2.csv", sep = ";")
colnames(ems_smc) <- c("FinalTPID", "WhyRecommended", "Recommendation", "Propensity", "Conversation")
ems_smc <- ems_smc %>% select(FinalTPID, Recommendation, Propensity)
ems_smc <- inner_join(ems_smc, smc)
ems_smc$Revenue <- 300 * 14.80 * 12 * (ems_smc$Propensity/100)
nrow(ems_smc) # 6294
sum(ems_smc$Revenue)  # 15,116,202

ca_smc <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\FinalDROutputv2.csv") 
ca_smc <- ca_smc %>% select(FinalTPID, Recommendation, Propensity)
ca_smc <- inner_join(ca_smc, smc)
ca_smc$Revenue <- 300 * 32 * 12 * (ca_smc$Propensity)
nrow(ca_smc) #8078
sum(ca_smc$Revenue)  # 42M

e5_smc <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\DROutput_ME5_63020_non1A1B.csv")
e5_smc <- e5_smc %>% select(FinalTPID, Recommendation, Propensity)
e5_smc <- inner_join(e5_smc, smc)
e5_smc$Revenue <- 300 * 57 * 12 * (e5_smc$Propensity/100)
nrow(e5_smc) #3194
sum(e5_smc$Revenue)  # 23M
