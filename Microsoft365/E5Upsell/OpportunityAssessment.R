
# for all enterprise accounts, pull total seats, total M365 seats
m365seats <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\Opportunity\\M365seats.csv")
colnames(m365seats)[1] <- "TPID"
m365seats <- m365seats %>% filter(!is.na(TPID))

m365seats <- m365seats %>% dplyr::select(TPID, TotalM365, Microsoft.365.E5.Enterprise, TotalME3Seats)

#get total pau
library(CosmosToR)
vc <- vc_connection('https://cosmos14.osdinfra.net/cosmos/ACE.proc/')
streamPath <- "local/Projects/ROI/TenantProfile.ss"
pau <- ss_all(vc, streamPath) 
pau <- pau %>% group_by(Tpid) %>% summarise(PAU = sum(PaidAvailableUnits))
colnames(pau)[1] <- "TPID"
pcib <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\Opportunity\\PCIB.csv")
colnames(pcib)[1] <- "TPID"

#area/industry
areaindus <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\Opportunity\\AreaIndus.csv")
colnames(areaindus)[1] <- "TPID"
areaindus <- areaindus %>% select(TPID, AreaName)

# get the averages for all enterprise for that seat size and that area 
# areaindus has all enterprise accounts 
all_ent <- areaindus %>% select(TPID, AreaName)

all_ent <- inner_join(all_ent, pcib, by = "TPID")
all_ent[is.na(all_ent)] <- 0
all_ent$sizebucket <- case_when(all_ent$ADJ.PCIB == 0 ~ "0", 
                                  all_ent$ADJ.PCIB > 0 & all_ent$ADJ.PCIB < 1000 ~ "1-1000",
                                  all_ent$ADJ.PCIB >= 1000 & all_ent$ADJ.PCIB < 3000 ~ "1000-3000",
                                  all_ent$ADJ.PCIB >= 3000 & all_ent$ADJ.PCIB < 7000 ~ "3000-7000",
                                  all_ent$ADJ.PCIB >= 7000 & all_ent$ADJ.PCIB < 15000 ~ "7000-15000",
                                  all_ent$ADJ.PCIB >= 15000 & all_ent$ADJ.PCIB < 30000 ~ "15000-30000",
                                  all_ent$ADJ.PCIB >30000 ~ ">30000")
                                  
all_ent <- left_join(all_ent, m365seats, by = "TPID")
all_ent$Microsoft.365.E5.Enterprise <- ifelse(all_ent$Microsoft.365.E5.Enterprise == "", 0, all_ent$Microsoft.365.E5.Enterprise)
all_ent$Microsoft.365.E5.Enterprise <- as.numeric(all_ent$Microsoft.365.E5.Enterprise)
all_ent$TotalM365 <- as.numeric(all_ent$TotalM365)
all_ent$TotalME3Seats <- as.numeric(all_ent$TotalME3Seats)
all_ent[is.na(all_ent)] <- 0

#only keep ones which are ME5 and M already?
all_ent1 <- all_ent %>% filter(Microsoft.365.E5.Enterprise > 250)
data1 <- all_ent1 %>% group_by(AreaName, sizebucket) %>% summarise(AvgME5Seats = mean(Microsoft.365.E5.Enterprise), 
                                                                   MedianME5Seats = median(Microsoft.365.E5.Enterprise)) 

all_ent2 <- all_ent %>% filter(TotalM365 > 1000)
data2 <- all_ent2 %>% group_by(AreaName, sizebucket) %>% summarise(AvgME3Seats = mean(TotalME3Seats),
                                                                    AvgMSeats = mean(TotalM365))


                                                                
# for the e5 and nca output - get the seat size and area bucket 

nca_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\M365EnterpriseCAOutput.csv")
colnames(nca_output)[1] <- "TPID"
nca_output <- nca_output %>% filter(Propensity != "L")

e5_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\FullOutput.csv")
colnames(e5_output)[1] <- "TPID"
e5_output <- e5_output %>% filter(Propensity != "Account shows low propensity to upsell to M365 E5")

e5_output <- inner_join(e5_output, all_ent, by = "TPID")
e5_output <- inner_join(e5_output, data1, by = c("AreaName", "sizebucket"))
nca_output <- inner_join(nca_output, all_ent, by = "TPID")
nca_output <- inner_join(nca_output, data2, by = c("AreaName", "sizebucket"))

write.csv(e5_output, "C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\OpportunityE5.csv", row.names = FALSE)
write.csv(nca_output, "C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\OpportunityCA.csv", row.names = FALSE)

#TODO - need to get opportunity count for E5 and NCA models.
# while doing for NCA - remove E5 ones 
