m365seats <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\Opportunity\\LicensingPosition.csv")
colnames(m365seats)[1] <- "TPID"
m365seats$TotalM365 <- m365seats$Sum.of.M365.E5 + m365seats$Sum.of.M365.E3
  
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
                                all_ent$ADJ.PCIB >= 30000 ~ ">30000")

all_ent <- left_join(all_ent, m365seats, by = "TPID")
all_ent[is.na(all_ent)] <- 0

all_ent1 <- all_ent %>% filter(Sum.of.M365.E5 > 0) #have multiple versions - one for 0, one for 250, one with no filter
data1 <- all_ent1 %>% group_by(AreaName, sizebucket) %>% summarise(AvgME5Seats = mean(Sum.of.M365.E5), 
                                                                   MedianME5Seats = median(Sum.of.M365.E5), count = n()) 

all_ent2 <- all_ent %>% filter(TotalM365 > 1000) #have multiple versions - one for 0, one for 1000, one with no filter
data2 <- all_ent2 %>% group_by(AreaName, sizebucket) %>% summarise(AvgME3Seats = mean(Sum.of.M365.E3),
                                                                   MedianME3Seats = median(Sum.of.M365.E3),
                                                                   AvgMSeats = mean(TotalM365),
                                                                   MedianMSeats = mean(TotalM365), 
                                                                   count = n())


# get the averages for the e5 and nca propensity tpids 

nca_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\M365EnterpriseCAOutput.csv")
colnames(nca_output)[1] <- "TPID"
nca_output <- nca_output %>% filter(Propensity != "L")

e5_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\FullOutput.csv")
colnames(e5_output)[1] <- "TPID"
e5_output <- e5_output %>% filter(Propensity != "Account shows low propensity to upsell to M365 E5")

e5_output$TPID <- as.character(e5_output$TPID)
e5_output <- inner_join(e5_output, all_ent, by = "TPID")
e5_output <- inner_join(e5_output, data1, by = c("AreaName", "sizebucket"))

nca_output$TPID <- as.character(nca_output$TPID)
nca_output <- inner_join(nca_output, all_ent, by = "TPID")
nca_output <- inner_join(nca_output, data1, by = c("AreaName", "sizebucket"))

write.csv(e5_output, "C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\OpportunityE5Final.csv", row.names = FALSE)
write.csv(nca_output, "C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\OpportunityCAFinal.csv", row.names = FALSE)


# Combine with Seat expansion and New Customer Adds Output

seat_expansion <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\SeatExpansion_Output.csv")
seat_expansion <- seat_expansion %>% filter(delta_flag == "expansion")

e5_output_1 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\Opportunity\\OpportunityE5GT250.csv")
e5_output_2 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\Opportunity\\OpportunityE5GT0.csv")
e5_output_3 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\Opportunity\\OpportunityE5Nofilter.csv")

e5_output_1$Delta <- e5_output_1$AvgME5Seats - e5_output_1$`Sum.of.M365.E5`
e5_output_1 <- left_join(e5_output_1, seat_expansion, by = "TPID")
e5_output_1$FinalSeatsGT250 <- ifelse(is.na(e5_output_1$Thresholds), e5_output_1$Delta, e5_output_1$Thresholds)
e5_output_1 <- e5_output_1 %>% dplyr::select(TPID, FinalSeatsGT250)

e5_output_2 <- left_join(e5_output_2, seat_expansion, by = "TPID")
e5_output_2$FinalSeatsGT0 <- ifelse(is.na(e5_output_2$Thresholds), e5_output_2$AvgME5Seats, e5_output_2$Thresholds)
e5_output_2 <- e5_output_2 %>% dplyr::select(TPID, FinalSeatsGT0)

e5_output_3 <- left_join(e5_output_3, seat_expansion, by = "TPID")
e5_output_3$FinalSeatsNofilter <- ifelse(is.na(e5_output_3$Thresholds), e5_output_3$AvgME5Seats, e5_output_3$Thresholds)
e5_output_3 <- e5_output_3 %>% dplyr::select(TPID, FinalSeatsNofilter)

final_e5 <- inner_join(e5_output_1, e5_output_2, by = "TPID") %>% inner_join(., e5_output_3, by = "TPID")


#nca 

# tpids that are only nca not e5 
a <- data.frame(setdiff(nca_output_1$TPID, e5_output_1$TPID))
colnames(a)[1] <- "TPID"

nca_output_1 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\Opportunity\\OpportunityCAGT1000.csv")
nca_output_2 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\Opportunity\\OpportunityCAGT0.csv")
nca_output_3 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\Opportunity\\OpportunityCANofilter.csv")

nca_output_1 <- inner_join(nca_output_1, a, by = "TPID")
nca_output_1 <- left_join(nca_output_1, seat_expansion, by = "TPID")
nca_output_1$FinalSeatsME3GT1000 <- ifelse(is.na(nca_output_1$Thresholds), nca_output_1$AvgME3Seats, nca_output_1$Thresholds)
nca_output_1$FinalSeatsMGT1000 <- ifelse(is.na(nca_output_1$Thresholds), nca_output_1$AvgMSeats, nca_output_1$Thresholds)
nca_output_1 <- nca_output_1 %>% dplyr::select(TPID, FinalSeatsME3GT1000, FinalSeatsMGT1000)

nca_output_2 <- inner_join(nca_output_2, a, by = "TPID")
nca_output_2 <- left_join(nca_output_2, seat_expansion, by = "TPID")
nca_output_2$FinalSeatsME3GT0 <- ifelse(is.na(nca_output_2$Thresholds), nca_output_2$AvgME3Seats, nca_output_2$Thresholds)
nca_output_2$FinalSeatsMGT0 <- ifelse(is.na(nca_output_2$Thresholds), nca_output_2$AvgMSeats, nca_output_2$Thresholds)
nca_output_2 <- nca_output_2 %>% dplyr::select(TPID, FinalSeatsME3GT0, FinalSeatsMGT0)

nca_output_3 <- inner_join(nca_output_3, a, by = "TPID")
nca_output_3 <- left_join(nca_output_3, seat_expansion, by = "TPID")
nca_output_3$FinalSeatsME3GTNofilter <- ifelse(is.na(nca_output_3$Thresholds), nca_output_3$AvgME3Seats, nca_output_3$Thresholds)
nca_output_3$FinalSeatsMGTNofilter <- ifelse(is.na(nca_output_3$Thresholds), nca_output_3$AvgMSeats, nca_output_3$Thresholds)
nca_output_3 <- nca_output_3 %>% dplyr::select(TPID, FinalSeatsME3GTNofilter, FinalSeatsMGTNofilter)

final_e5 <- inner_join(e5_output_1, e5_output_2, by = "TPID") %>% inner_join(., e5_output_3, by = "TPID")


# for different profiles of customers #
nca_profiles <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\NCA_OutputProfiles.csv")
colnames(nca_profiles)[1] <- "TPID"
data <- inner_join(nca_profiles, nca_output_1, by = "TPID")
data$Profile <- as.character(data$Profile)
a <- data %>% filter(Profile == "(3) O365 Suites")



