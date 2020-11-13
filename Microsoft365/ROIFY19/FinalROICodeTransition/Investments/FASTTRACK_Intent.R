library(data.table)
library(dplyr)

ft7 <- fread("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Intent\\20180731.csv")
colnames(ft7)[7] <- "Status"
ft7$ym <- "201907"
ft8 <- fread("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Intent\\20180831.csv")
colnames(ft8)[7] <- "Status"
ft8$ym <- "201908"
ft9 <- fread("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Intent\\20180930.csv")
colnames(ft9)[7] <- "Status"
ft9$ym <- "201909"
ft10 <- fread("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Intent\\20181031.csv")
colnames(ft10)[7] <- "Status"
ft10$ym <- "201910"
ft11 <- fread("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Intent\\20181130.csv")
colnames(ft11)[7] <- "Status"
ft12 <- fread("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Intent\\20181231.csv")
colnames(ft12) <- colnames(ft11)
ft11$ym <- "201911"
ft12$ym <- "201912"

ft_data <- rbind(ft7, ft8, ft9, ft10, ft11, ft12)


# All workloads
ft_all <- ft_data %>% filter(ServiceName == "CSMAU" & Status %in% c("Blocked", "No Intent"))
ft_all <- ft_all  %>% select(TenantID, EntitlementSeats, Status, FTC_PreferredDenominator, ym)
ft_all_1 <- filter(ft_all, Status == "Blocked")
colnames(ft_all_1)[2] <- "BlockedSeats"
ft_all_1 <- ft_all_1 %>% select(TenantID, BlockedSeats, FTC_PreferredDenominator, ym)
ft_all_2 <- filter(ft_all, Status == "No Intent")
colnames(ft_all_2)[2] <- "NoIntentSeats"
ft_all_2 <- ft_all_2 %>% select(TenantID, NoIntentSeats, FTC_PreferredDenominator, ym)
ft_all_3 <- full_join(ft_all_1, ft_all_2, by = c("TenantID", "ym"))
ft_all_3[is.na(ft_all_3)] <- 0
ft_all_3$Denom <- max(ft_all_3$FTC_PreferredDenominator.x, ft_all_3$FTC_PreferredDenominator.y)
ft_all_3$BlockedPerc <- ft_all_3$BlockedSeats/ft_all_3$Denom
ft_all_3$NoIntentPerc <- ft_all_3$NoIntentSeats/ft_all_3$Denom
ft_all_3$TenantId <- tolower(ft_all_3$TenantID)
ft_all <- ft_all_3 %>% select(TenantId, BlockedSeats, NoIntentSeats, BlockedPerc, NoIntentPerc, ym)

write.csv(ft_all, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FT_Intent.csv", row.names = FALSE)

# Teams
ft_all <- ft_data %>% filter(ServiceName == "Teams" & Status %in% c("Blocked", "No Intent"))
ft_all <- ft_all  %>% select(TenantID, EntitlementSeats, Status, FTC_PreferredDenominator, ym)
ft_all_1 <- filter(ft_all, Status == "Blocked")
colnames(ft_all_1)[2] <- "BlockedSeats"
ft_all_1 <- ft_all_1 %>% select(TenantID, BlockedSeats, FTC_PreferredDenominator, ym)
ft_all_2 <- filter(ft_all, Status == "No Intent")
colnames(ft_all_2)[2] <- "NoIntentSeats"
ft_all_2 <- ft_all_2 %>% select(TenantID, NoIntentSeats, FTC_PreferredDenominator, ym)
ft_all_3 <- full_join(ft_all_1, ft_all_2, by = c("TenantID", "ym"))
ft_all_3[is.na(ft_all_3)] <- 0
ft_all_3$Denom <- max(ft_all_3$FTC_PreferredDenominator.x, ft_all_3$FTC_PreferredDenominator.y)
ft_all_3$BlockedPerc <- ft_all_3$BlockedSeats/ft_all_3$Denom
ft_all_3$NoIntentPerc <- ft_all_3$NoIntentSeats/ft_all_3$Denom
ft_all_3$TenantId <- tolower(ft_all_3$TenantID)
ft_all <- ft_all_3 %>% select(TenantId, BlockedSeats, NoIntentSeats, BlockedPerc, NoIntentPerc, ym)


write.csv(ft_all, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\FT_Intent.csv", row.names = FALSE)

# Core Workloads 
ft_all <- ft_data %>% filter(ServiceName %in% c("OfficeProPlus", "SharePoint", "OneDrive", "Exchange", "Teams", "Exchange: Outlook Mobile")
                             & Status %in% c("Blocked", "No Intent"))
ft_all <- ft_all  %>% select(TenantID, EntitlementSeats, Status, FTC_PreferredDenominator, ym)
ft_all[is.na(ft_all)] <- 0
ft_all <- ft_all %>% group_by(TenantID, Status, ym) %>% summarize(EntitlementSeats = sum(EntitlementSeats),
                                                              FTC_PreferredDenominator = sum(FTC_PreferredDenominator))

ft_all_1 <- filter(ft_all, Status == "Blocked")
colnames(ft_all_1)[4] <- "BlockedSeats"
ft_all_1 <- ft_all_1 %>% ungroup() %>% select(TenantID, BlockedSeats, FTC_PreferredDenominator, ym)
ft_all_2 <- filter(ft_all, Status == "No Intent")
colnames(ft_all_2)[4] <- "NoIntentSeats"
ft_all_2 <- ft_all_2 %>% ungroup() %>% select(TenantID, NoIntentSeats, FTC_PreferredDenominator, ym)
ft_all_3 <- full_join(ft_all_1, ft_all_2, by = c("TenantID", "ym"))
ft_all_3[is.na(ft_all_3)] <- 0
ft_all_3$Denom <- max(ft_all_3$FTC_PreferredDenominator.x, ft_all_3$FTC_PreferredDenominator.y)
ft_all_3$BlockedPerc <- ft_all_3$BlockedSeats/ft_all_3$Denom
ft_all_3$NoIntentPerc <- ft_all_3$NoIntentSeats/ft_all_3$Denom
ft_all_3$TenantId <- tolower(ft_all_3$TenantID)
ft_all <- ft_all_3 %>% select(TenantId, BlockedSeats, NoIntentSeats, BlockedPerc, NoIntentPerc, ym)

write.csv(ft_all, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CoreWorkloads\\FT_Intent.csv", row.names = FALSE)

#AADP
ft_all <- ft_data %>% filter(ServiceName == "AADP" & Status %in% c("Blocked", "No Intent"))
ft_all <- ft_all  %>% select(TenantID, EntitlementSeats, Status, FTC_PreferredDenominator, ym)
ft_all_1 <- filter(ft_all, Status == "Blocked")
colnames(ft_all_1)[2] <- "BlockedSeats"
ft_all_1 <- ft_all_1 %>% select(TenantID, BlockedSeats, FTC_PreferredDenominator, ym)
ft_all_2 <- filter(ft_all, Status == "No Intent")
colnames(ft_all_2)[2] <- "NoIntentSeats"
ft_all_2 <- ft_all_2 %>% select(TenantID, NoIntentSeats, FTC_PreferredDenominator, ym)
ft_all_3 <- full_join(ft_all_1, ft_all_2, by = c("TenantID", "ym"))
ft_all_3[is.na(ft_all_3)] <- 0
ft_all_3$Denom <- max(ft_all_3$FTC_PreferredDenominator.x, ft_all_3$FTC_PreferredDenominator.y)
ft_all_3$BlockedPerc <- ft_all_3$BlockedSeats/ft_all_3$Denom
ft_all_3$NoIntentPerc <- ft_all_3$NoIntentSeats/ft_all_3$Denom
ft_all_3$TenantId <- tolower(ft_all_3$TenantID)
ft_all <- ft_all_3 %>% select(TenantId, BlockedSeats, NoIntentSeats, BlockedPerc, NoIntentPerc, ym)
write.csv(ft_all, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\AADP\\Investments\\FT_Intent.csv", row.names = FALSE)
