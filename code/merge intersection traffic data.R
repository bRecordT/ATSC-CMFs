#merge segment data
rm(list=ls())
gc()
setwd("G:/*****/Final Dataset")
library(readxl)
library(dplyr)
library(sqldf)

#merge traffic info
data <- read.csv("basic data.csv", header = T, stringsAsFactors = F)
rms_traf19 <- read.csv("PaTraffic2019_05.csv", header = T, stringsAsFactors = F)
rms_traf19 <- rms_traf19[,c(3, 4, 7:10, 15, 42)]
#merge data for each leg
c1 <- c("ID", "CTY1", "SR1", "SEG1", "OFF1", "SIDE_IND1", "SEG_LNGTH_1")
c2 <- c("ID", "CTY2", "SR2", "SEG2", "OFF2", "SIDE_IND2", "SEG_LNGTH_2")
c3 <- c("ID", "CTY3", "SR3", "SEG3", "OFF3", "SIDE_IND3", "SEG_LNGTH_3")
c4 <- c("ID", "CTY4", "SR4", "SEG4", "OFF4", "SIDE_IND4", "SEG_LNGTH_4")
c5 <- c("ID", "CTY5", "SR5", "SEG5", "OFF5", "SIDE_IND5", "SEG_LNGTH_5")
c6 <- c("ID", "CTY6", "SR6", "SEG6", "OFF6", "SIDE_IND6", "SEG_LNGTH_6")
c7 <- c("ID", "CTY7", "SR7", "SEG7", "OFF7", "SIDE_IND7", "SEG_LNGTH_7")
c8 <- c("ID", "CTY8", "SR8", "SEG8", "OFF8", "SIDE_IND8", "SEG_LNGTH_8")
c9 <- c("ID", "CTY9", "SR9", "SEG9", "OFF9", "SIDE_IND9", "SEG_LNGTH_9")
c10 <- c("ID", "CTY10", "SR10", "SEG10", "OFF10", "SIDE_IND10", "SEG_LNGTH_10")
clist <- list(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)

for (i in 1:10) {
  assign(paste0("leg_crash", i), data.frame())
}
leg_crash <- list(leg_crash1, leg_crash2, leg_crash3, leg_crash4, leg_crash5,
                  leg_crash6, leg_crash7, leg_crash8, leg_crash9, leg_crash10)

for(i in 1:10){
  leg <- data[clist[[i]]]
  names(leg) <- c("ID", "cty", "sr", "seg", "offset", "SIDE_IND", "length")
  leg_traf <- sqldf("select d.*, t.* from leg d left join rms_traf19 t on d.cty = t.CTY_CODE AND d.sr = t.ST_RT_NO
                     AND d.SIDE_IND = t.SIDE_IND AND d.SEG >= t.SEG_BGN AND d.SEG <= t.SEG_END 
                     order by d.cty, d.sr, d.seg, t.OFFSET_BGN")
  leg_traf$feature_length <- ifelse(leg_traf$SEG_BGN == leg_traf$SEG_END, leg_traf$OFFSET_END - leg_traf$OFFSET_BGN,
                                    ifelse(leg_traf$seg == leg_traf$SEG_BGN, leg_traf$length - leg_traf$OFFSET_BGN,
                                           ifelse(leg_traf$seg == leg_traf$SEG_END, leg_traf$OFFSET_END, leg_traf$length)))
  leg_traf <- sqldf("select leg_traf.* from leg_traf where (cty, sr, seg, feature_length) 
                     in (select cty, sr, seg, max(feature_length) from leg_traf group by cty, sr, seg) 
                     union
                     select leg_traf.* from leg_traf where feature_length is null")
  leg_crash[[i]] <- leg_traf
  leg_crash[[i]] <- arrange(leg_crash[[i]], ID)
  names(leg_crash[[i]]) <- paste0(names(leg_crash[[i]]), "_", i)
}

data_crash <- do.call(cbind, leg_crash)
var_remove <- c("length", "SIDE_IND", "ST_RT_NO", "CTY_CODE", "SEG_BGN", "OFFSET_BGN", "SEG_END",
                "OFFSET_END", "feature_length")
for (i in 1: length(var_remove)){
  data_crash <- select(data_crash, -starts_with(var_remove[i]))
}

data <- data_crash
data2 <- read.csv("merge intersection/merge basic info.csv", header = T, stringsAsFactors = F)
data <- merge(data2, data, by.x = 'ID', by.y = 'ID_1')

#determine approach info
data <- data %>% mutate(SPEED_LIMIT1 = ifelse(SPEED_LIMIT1 == 0, NA, SPEED_LIMIT1),
                        SPEED_LIMIT2 = ifelse(SPEED_LIMIT2 == 0, NA, SPEED_LIMIT2),
                        SPEED_LIMIT3 = ifelse(SPEED_LIMIT3 == 0, NA, SPEED_LIMIT3),
                        SPEED_LIMIT4 = ifelse(SPEED_LIMIT4 == 0, NA, SPEED_LIMIT4),
                        SPEED_LIMIT5 = ifelse(SPEED_LIMIT5 == 0, NA, SPEED_LIMIT5),
                        SPEED_LIMIT6 = ifelse(SPEED_LIMIT6 == 0, NA, SPEED_LIMIT6),
                        SPEED_LIMIT7 = ifelse(SPEED_LIMIT7 == 0, NA, SPEED_LIMIT7),
                        SPEED_LIMIT8 = ifelse(SPEED_LIMIT8 == 0, NA, SPEED_LIMIT8),
                        SPEED_LIMIT9 = ifelse(SPEED_LIMIT9 == 0, NA, SPEED_LIMIT9),
                        SPEED_LIMIT10 = ifelse(SPEED_LIMIT10 == 0, NA, SPEED_LIMIT10),
                        TOTAL_WIDT1 = ifelse(TOTAL_WIDT1 == 0, NA, TOTAL_WIDT1),
                        TOTAL_WIDT2 = ifelse(TOTAL_WIDT2 == 0, NA, TOTAL_WIDT2),
                        TOTAL_WIDT3 = ifelse(TOTAL_WIDT3 == 0, NA, TOTAL_WIDT3),
                        TOTAL_WIDT4 = ifelse(TOTAL_WIDT4 == 0, NA, TOTAL_WIDT4),
                        TOTAL_WIDT5 = ifelse(TOTAL_WIDT5 == 0, NA, TOTAL_WIDT5),
                        TOTAL_WIDT6 = ifelse(TOTAL_WIDT1 == 0, NA, TOTAL_WIDT6),
                        TOTAL_WIDT7 = ifelse(TOTAL_WIDT2 == 0, NA, TOTAL_WIDT7),
                        TOTAL_WIDT8 = ifelse(TOTAL_WIDT3 == 0, NA, TOTAL_WIDT8),
                        TOTAL_WIDT9 = ifelse(TOTAL_WIDT4 == 0, NA, TOTAL_WIDT9),
                        TOTAL_WIDT10 = ifelse(TOTAL_WIDT5 == 0, NA, TOTAL_WIDT10)) %>% 
  mutate(aadt_leg1 = rowSums(.[,c("CUR_AADT_1", "CUR_AADT_2")], na.rm = T),
         aadt_leg2 = rowSums(.[,c("CUR_AADT_3", "CUR_AADT_4")], na.rm = T),
         aadt_leg3 = rowSums(.[,c("CUR_AADT_5", "CUR_AADT_6")], na.rm = T),
         aadt_leg4 = rowSums(.[,c("CUR_AADT_7", "CUR_AADT_8")], na.rm = T),
         aadt_leg5 = rowSums(.[,c("CUR_AADT_9", "CUR_AADT_10")], na.rm = T),
         lane_leg1 = rowSums(.[,c("LANE_CNT1", "LANE_CNT2")], na.rm = T),
         lane_leg2 = rowSums(.[,c("LANE_CNT3", "LANE_CNT4")], na.rm = T),
         lane_leg3 = rowSums(.[,c("LANE_CNT5", "LANE_CNT6")], na.rm = T),
         lane_leg4 = rowSums(.[,c("LANE_CNT7", "LANE_CNT8")], na.rm = T),
         lane_leg5 = rowSums(.[,c("LANE_CNT9", "LANE_CNT10")], na.rm = T),
         width_leg1 = rowSums(.[,c("TOTAL_WIDT1", "TOTAL_WIDT2")], na.rm = T),
         width_leg2 = rowSums(.[,c("TOTAL_WIDT3", "TOTAL_WIDT4")], na.rm = T),
         width_leg3 = rowSums(.[,c("TOTAL_WIDT5", "TOTAL_WIDT6")], na.rm = T),
         width_leg4 = rowSums(.[,c("TOTAL_WIDT7", "TOTAL_WIDT8")], na.rm = T),
         width_leg5 = rowSums(.[,c("TOTAL_WIDT9", "TOTAL_WIDT10")], na.rm = T),
         ls_pave_leg1 = ifelse(is.na(SEG2), L_S_PAVE1, L_S_PAVE2),
         ls_pave_leg2 = ifelse(is.na(SEG4), L_S_PAVE3, L_S_PAVE4),
         ls_pave_leg3 = ifelse(is.na(SEG6), L_S_PAVE5, L_S_PAVE6),
         ls_pave_leg4 = ifelse(is.na(SEG8), L_S_PAVE7, L_S_PAVE8),
         ls_pave_leg5 = ifelse(is.na(SEG10), L_S_PAVE9, L_S_PAVE10),
         rs_pave_leg1 = R_S_PAVE1,
         rs_pave_leg2 = R_S_PAVE3,
         rs_pave_leg3 = R_S_PAVE5,
         rs_pave_leg4 = R_S_PAVE7,
         rs_pave_leg5 = R_S_PAVE9,
         ls_width_leg1 = ifelse(is.na(SEG2), L_S_TOTL1, L_S_TOTL2),
         ls_width_leg2 = ifelse(is.na(SEG4), L_S_TOTL3, L_S_TOTL4),
         ls_width_leg3 = ifelse(is.na(SEG6), L_S_TOTL5, L_S_TOTL6),
         ls_width_leg4 = ifelse(is.na(SEG8), L_S_TOTL7, L_S_TOTL8),
         ls_width_leg5 = ifelse(is.na(SEG10), L_S_TOTL9, L_S_TOTL10),
         rs_width_leg1 = R_S_TOTL1,
         rs_width_leg2 = R_S_TOTL3,
         rs_width_leg3 = R_S_TOTL5,
         rs_width_leg4 = R_S_TOTL7,
         rs_width_leg5 = R_S_TOTL9,
         speed_leg1 = pmax(SPEED_LIMIT1, SPEED_LIMIT2, na.rm = T),
         speed_leg2 = pmax(SPEED_LIMIT3, SPEED_LIMIT4, na.rm = T),
         speed_leg3 = pmax(SPEED_LIMIT5, SPEED_LIMIT6, na.rm = T),
         speed_leg4 = pmax(SPEED_LIMIT7, SPEED_LIMIT8, na.rm = T),
         speed_leg5 = pmax(SPEED_LIMIT9, SPEED_LIMIT10, na.rm = T)) %>% 
  mutate(aadt_leg1 = ifelse(aadt_leg1 == 0, NA, aadt_leg1),
         aadt_leg2 = ifelse(aadt_leg2 == 0, NA, aadt_leg2),
         aadt_leg3 = ifelse(aadt_leg3 == 0, NA, aadt_leg3),
         aadt_leg4 = ifelse(aadt_leg4 == 0, NA, aadt_leg4),
         aadt_leg5 = ifelse(aadt_leg5 == 0, NA, aadt_leg5),
         lane_leg1 = ifelse(lane_leg1 == 0, NA, lane_leg1),
         lane_leg2 = ifelse(lane_leg2 == 0, NA, lane_leg2),
         lane_leg3 = ifelse(lane_leg3 == 0, NA, lane_leg3),
         lane_leg4 = ifelse(lane_leg4 == 0, NA, lane_leg4),
         lane_leg5 = ifelse(lane_leg5 == 0, NA, lane_leg5),
         width_leg1 = ifelse(width_leg1 == 0, NA, width_leg1),
         width_leg2 = ifelse(width_leg2 == 0, NA, width_leg2),
         width_leg3 = ifelse(width_leg3 == 0, NA, width_leg3),
         width_leg4 = ifelse(width_leg4 == 0, NA, width_leg4),
         width_leg5 = ifelse(width_leg5 == 0, NA, width_leg5)) %>%
  mutate(aadt_approach1 = rowMeans(.[,c("aadt_leg1", "aadt_leg2")], na.rm = T),
         aadt_approach2 = rowMeans(.[,c("aadt_leg3", "aadt_leg4")], na.rm = T),
         lane_approach1 = rowMeans(.[,c("lane_leg1", "lane_leg2")], na.rm = T),
         lane_approach2 = rowMeans(.[,c("lane_leg3", "lane_leg4")], na.rm = T),
         width_approach1 = rowMeans(.[,c("width_leg1", "width_leg2")], na.rm = T),
         width_approach2 = rowMeans(.[,c("width_leg3", "width_leg4")], na.rm = T),
         ls_pave_approach1 = rowMeans(.[,c("ls_pave_leg1", "ls_pave_leg2")], na.rm = T),
         ls_pave_approach2 = rowMeans(.[,c("ls_pave_leg3", "ls_pave_leg4")], na.rm = T),
         rs_pave_approach1 = rowMeans(.[,c("rs_pave_leg1", "rs_pave_leg2")], na.rm = T),
         rs_pave_approach2 = rowMeans(.[,c("rs_pave_leg3", "rs_pave_leg4")], na.rm = T),
         ls_width_approach1 = rowMeans(.[,c("ls_width_leg1", "ls_width_leg2")], na.rm = T),
         ls_width_approach2 = rowMeans(.[,c("ls_width_leg3", "ls_width_leg4")], na.rm = T),
         rs_width_approach1 = rowMeans(.[,c("rs_width_leg1", "rs_width_leg2")], na.rm = T),
         rs_width_approach2 = rowMeans(.[,c("rs_width_leg3", "rs_width_leg4")], na.rm = T),
         speed_approach1 = pmax(speed_leg1, speed_leg2, na.rm = T),
         speed_approach2 = pmax(speed_leg3, speed_leg4, na.rm = T)) %>%
  mutate_at(684:699, list(~round(.))) %>%
  mutate(Major12 = ifelse(X3LEG == 1, 1, 
                          ifelse(is.na(aadt_approach2) | aadt_approach1 >= aadt_approach2, 1, 0)),
         Major34 = ifelse(X3LEG == 0 & (!is.na(aadt_approach2)) & aadt_approach1 < aadt_approach2, 1, 0),
         major_lane = ifelse(Major12 == 1, lane_approach1, lane_approach2),
         minor_lane = ifelse(Major12 == 1, lane_approach2, lane_approach1),
         major_width = ifelse(Major12 == 1, width_approach1, width_approach2),
         minor_width = ifelse(Major12 == 1, width_approach2, width_approach1),
         major_ls_pave = ifelse(Major12 == 1, ls_pave_approach1, ls_pave_approach2),
         minor_ls_pave = ifelse(Major12 == 1, ls_pave_approach2, ls_pave_approach1),
         major_rs_pave = ifelse(Major12 == 1, rs_pave_approach1, rs_pave_approach2),
         minor_rs_pave = ifelse(Major12 == 1, rs_pave_approach2, rs_pave_approach1),
         major_ls_width = ifelse(Major12 == 1, ls_width_approach1, ls_width_approach2),
         minor_ls_width = ifelse(Major12 == 1, ls_width_approach2, ls_width_approach1),
         major_rs_width = ifelse(Major12 == 1, rs_width_approach1, rs_width_approach2),
         minor_rs_width = ifelse(Major12 == 1, rs_width_approach2, rs_width_approach1),
         major_speed = ifelse(Major12 == 1, speed_approach1, speed_approach2),
         minor_speed = ifelse(Major12 == 1, speed_approach2, speed_approach1)
         )

write.csv(data, file = "merge intersection/basic with traffic info.csv", row.names = F)

